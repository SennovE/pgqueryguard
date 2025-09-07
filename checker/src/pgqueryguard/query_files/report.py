import html
import json
import textwrap
from typing import Any
from datetime import datetime

from pgqueryguard.outer_database.advice import Advice
from pgqueryguard.outer_database.count_resourses import CostProfile


def fmt_num(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")


def fmt_float(x: float, digits: int = 2) -> str:
    return f"{x:,.{digits}f}".replace(",", " ")


def fmt_bytes(n: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    s = float(n)
    for u in units:
        if s < 1024 or u == units[-1]:
            return f"{fmt_float(s, 2)} {u}"
        s /= 1024.0


def risk_from_profile(p: CostProfile) -> tuple[str, str]:
    high = (p.est_pages >= 500_000) or (p.est_memory_bytes >= 1_000_000_000)
    med = (p.est_pages >= 100_000) or (p.est_memory_bytes >= 256_000_000)
    if high:
        return "HIGH", "Оценка высокая: много страниц или память > ~1 ГБ"
    if med:
        return "MED", "Оценка средняя: заметный объём данных/памяти"
    return "LOW", "Оценка низкая: прогноз умеренный"


def _escape(s: Any) -> str:
    return html.escape(str(s), quote=True)


def _node_kv(n: dict[str, Any], keys: list[str]) -> str:
    parts = []
    for k in keys:
        v = n.get(k)
        if v is not None:
            parts.append(f'<span class="kv"><b>{_escape(k)}:</b> {_escape(v)}</span>')
    return " · ".join(parts)


def plan_to_tree_html(plan_json: dict[str, Any]) -> str:
    p = plan_json.get("Plan", plan_json)

    def render(n: dict[str, Any]) -> str:
        title = n.get("Node Type", "Node")
        header = _escape(title)
        meta = _node_kv(
            n, ["Relation Name", "Index Name", "Join Type", "Parallel Aware"]
        )
        costs = _node_kv(n, ["Startup Cost", "Total Cost", "Plan Rows", "Plan Width"])
        filt = n.get("Filter")
        keys = []
        if n.get("Sort Key"):
            keys.append(
                f'<span class="kv"><b>Sort Key:</b> {_escape(n["Sort Key"])}</span>'
            )
        if n.get("Hash Cond"):
            keys.append(
                f'<span class="kv"><b>Hash Cond:</b> {_escape(n["Hash Cond"])}</span>'
            )
        if n.get("Merge Cond"):
            keys.append(
                f'<span class="kv"><b>Merge Cond:</b> {_escape(n["Merge Cond"])}</span>'
            )
        keys_html = " · ".join(keys)
        filt_html = (
            f'<div class="filter">Filter: <code>{_escape(filt)}</code></div>'
            if filt
            else ""
        )

        children = n.get("Plans") or []
        kids_html = "".join(render(ch) for ch in children)
        has_kids = " has-kids" if children else ""

        return f"""
<li class="node{has_kids}">
  <details open>
    <summary>
      <span class="node-title">{header}</span>
      <span class="node-meta">{meta}</span>
      <span class="node-costs">{costs}</span>
      {'<span class="node-keys">' + keys_html + "</span>" if keys_html else ""}
    </summary>
    {filt_html}
    {('<ul class="tree">' + kids_html + "</ul>") if kids_html else ""}
  </details>
</li>"""

    return f'<ul class="tree root">{render(p)}</ul>'


def plan_nodes_table(plan_json: dict[str, Any]) -> str:
    p = plan_json.get("Plan", plan_json)
    rows_html = []

    def walk(n: dict[str, Any], depth: int = 0):
        rows_html.append(f"""
<tr>
  <td>{_escape("  " * depth + n.get("Node Type", ""))}</td>
  <td>{_escape(n.get("Relation Name", ""))}</td>
  <td>{_escape(n.get("Index Name", ""))}</td>
  <td class="num">{_escape(n.get("Startup Cost", ""))}</td>
  <td class="num">{_escape(n.get("Total Cost", ""))}</td>
  <td class="num">{_escape(n.get("Plan Rows", ""))}</td>
  <td class="num">{_escape(n.get("Plan Width", ""))}</td>
</tr>
""")
        for ch in n.get("Plans") or []:
            walk(ch, depth + 1)

    walk(p, 0)
    return f"""
<table class="nodes">
  <thead>
    <tr>
      <th>Node</th><th>Relation</th><th>Index</th>
      <th>Startup</th><th>Total</th><th>Rows</th><th>Width</th>
    </tr>
  </thead>
  <tbody>
    {"".join(rows_html)}
  </tbody>
</table>
"""


def advice_section(advice: list[Advice]) -> str:
    if not advice:
        return '<p class="muted">Рекомендации не найдены - план выглядит разумно.</p>'
    priority_to_class = {
        "high": "badge high",
        "medium": "badge med",
        "low": "badge low",
    }
    cards = []
    for a in advice:
        pri = priority_to_class.get(a.priority, "badge")
        ddl_block = ""
        if a.ddl:
            code = _escape(a.ddl.strip())
            ddl_block = f"""
<div class="ddl">
  <div class="ddl-head">DDL предложение</div>
  <pre><code>{code}</code></pre>
  <button class="copy" onclick="copyDDL(this)">Копировать</button>
</div>"""
        speed = (
            f'<span class="speed">⏩ Ожидаемое ускорение: {html.escape(a.est_speedup)}</span>'
            if a.est_speedup
            else ""
        )
        cards.append(f"""
<article class="card">
  <div class="card-top">
    <span class="{pri}">{a.priority.upper()}</span>
    {speed}
  </div>
  <div class="msg">{_escape(a.message)}</div>
  {ddl_block}
</article>""")
    return '<div class="cards">' + "".join(cards) + "</div>"


def write_html_report(
    filepath: str,
    plan_json: dict[str, Any],
    profile: CostProfile,
    advice: list[Advice],
    sql_text: str | None = None,
    db_dsn_label: str | None = None,
) -> None:
    risk_lvl, risk_note = risk_from_profile(profile)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql_block = (
        f'<pre class="sql"><code>{_escape(sql_text.strip())}</code></pre>'
        if sql_text
        else ""
    )
    warnings_html = "".join(f"<li>{_escape(w)}</li>" for w in (profile.warnings or []))
    if warnings_html:
        warnings_html = f'<ul class="warn-list">{warnings_html}</ul>'
    plan_tree = plan_to_tree_html(plan_json)
    nodes_table = plan_nodes_table(plan_json)
    advice_html = advice_section(advice)
    plan_raw_json = html.escape(json.dumps(plan_json, ensure_ascii=False, indent=2))

    css = """
:root {
  --bg: #0b1020; --fg: #E7ECF4; --muted:#9AA6B2; --card:#121a33; --acc:#7C9BFF;
  --ok:#3fb950; --med:#f2cc60; --high:#ff6b6b; --chip:#1b2447; --border:#22305b;
}
@media (prefers-color-scheme: light) {
  :root {
    --bg:#f7f9fc; --fg:#0c1220; --muted:#697586; --card:#ffffff; --acc:#3558ff;
    --ok:#0f9150; --med:#b88700; --high:#d63939; --chip:#eef2ff; --border:#e5e9f2;
  }
}
*{box-sizing:border-box} html,body{margin:0;padding:0;background:var(--bg);color:var(--fg);font:15px/1.5 Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial}
.container{max-width:1200px;margin:0 auto;padding:24px}
.header{display:flex;flex-wrap:wrap;align-items:center;gap:16px;margin-bottom:16px}
.h-title{font-size:22px;font-weight:700}
.h-sub{color:var(--muted)}
.badge{display:inline-block;padding:4px 8px;border-radius:999px;background:var(--chip);border:1px solid var(--border);font-weight:600;font-size:12px}
.badge.low{background:rgba(63,185,80,.12);color:var(--ok);border-color:rgba(63,185,80,.3)}
.badge.med{background:rgba(242,204,96,.12);color:var(--med);border-color:rgba(242,204,96,.35)}
.badge.high{background:rgba(255,107,107,.12);color:var(--high);border-color:rgba(255,107,107,.35)}
.chips{display:flex;gap:8px;flex-wrap:wrap}
.section{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:18px;margin:16px 0}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.kpi{background:var(--chip);border:1px solid var(--border);border-radius:12px;padding:12px}
.kpi .label{color:var(--muted);font-size:12px}
.kpi .value{font-size:18px;font-weight:700;margin-top:4px}
.warn{background:rgba(255,107,107,.08);border-color:rgba(255,107,107,.35)}
.warn .title{color:var(--high);font-weight:700;margin-bottom:6px}
.warn-list{margin:8px 0 0 18px}
.sql, pre code{white-space:pre-wrap;word-break:break-word}
pre{background:#0a0f1f1a;border:1px solid var(--border);border-radius:12px;padding:12px;overflow:auto}
.tree{list-style:none;padding-left:18px;margin:0}
.tree.root{padding-left:0}
.node summary{list-style:none;cursor:pointer;display:flex;flex-direction:column;gap:4px;padding:8px;border-radius:10px}
.node summary:hover{background:rgba(124,155,255,.08)}
.node-title{font-weight:700}
.node-meta, .node-costs, .node-keys{color:var(--muted);font-size:12px}
.filter{margin:8px 0;color:var(--muted)}
.nodes{width:100%;border-collapse:collapse}
.nodes th, .nodes td{border-bottom:1px solid var(--border);padding:8px 10px}
.nodes td.num{text-align:right}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px}
.card-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.msg{margin:6px 0 10px}
.ddl-head{font-size:12px;color:var(--muted);margin-bottom:4px}
.copy{display:inline-block;border:1px solid var(--border);background:var(--chip);color:var(--fg);border-radius:8px;padding:6px 10px;cursor:pointer}
.copy:active{transform:translateY(1px)}
.muted{color:var(--muted)}
.footer{color:var(--muted);font-size:12px;margin-top:8px;display:flex;gap:16px;flex-wrap:wrap}
.kv b{color:var(--muted);font-weight:600}
"""

    js = """
function copyDDL(btn){
  const pre = btn.previousElementSibling?.querySelector('code');
  if(!pre) return;
  const text = pre.innerText;
  navigator.clipboard.writeText(text).then(()=>{
    const old = btn.innerText;
    btn.innerText = "Скопировано ✓";
    setTimeout(()=>btn.innerText = old, 1200);
  });
}
"""

    html_doc = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SQL Advisor Report</title>
  <style>{css}</style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="h-title">SQL Advisor - отчёт анализа запроса</div>
      <div class="chips">
        <span class="badge">{_escape(db_dsn_label or "PostgreSQL (read-only)")}</span>
        <span class="badge">{_escape(now)}</span>
        <span class="badge {"high" if risk_lvl == "HIGH" else ("med" if risk_lvl == "MED" else "low")}">{_escape("RISK: " + risk_lvl)}</span>
      </div>
      <div class="h-sub">{_escape(risk_note)}</div>
    </div>

    {"<div class='section'><div class='kpi'><div class='label'>SQL</div></div>" + sql_block + "</div>" if sql_text else ""}

    <div class="section">
      <div class="grid">
        <div class="kpi"><div class="label">Total Cost</div><div class="value">{fmt_float(profile.total_cost)}</div></div>
        <div class="kpi"><div class="label">Оценка строк</div><div class="value">{fmt_num(profile.est_rows)}</div></div>
        <div class="kpi"><div class="label">Сканируемые данные</div><div class="value">{fmt_bytes(profile.est_bytes)}</div></div>
        <div class="kpi"><div class="label">Страницы (8KB)</div><div class="value">{fmt_num(profile.est_pages)}</div></div>
        <div class="kpi"><div class="label">Память (Sort/Hash)</div><div class="value">{fmt_bytes(profile.est_memory_bytes)}</div></div>
        <div class="kpi"><div class="label">Узлов в плане</div><div class="value">{fmt_num(len(profile.nodes))}</div></div>
        <div class="kpi"><div class="label">Предупреждений</div><div class="value">{fmt_num(len(profile.warnings))}</div></div>
      </div>
      {"<div class='section warn'><div class='title'>Предупреждения</div>" + warnings_html + "</div>" if profile.warnings else ""}
    </div>

    <div class="section">
      <h3>Дерево плана (EXPLAIN JSON)</h3>
      {plan_tree}
    </div>

    <div class="section">
      <h3>Узлы плана (сводная таблица)</h3>
      {nodes_table}
    </div>

    <div class="section">
      <h3>Рекомендации</h3>
      {advice_html}
    </div>

    <div class="section">
      <h3>EXPLAIN (FORMAT JSON) - исходник</h3>
      <pre><code>{plan_raw_json}</code></pre>
    </div>

    <div class="footer">
      <span>Сгенерировано офлайн-репортом</span>
      <span>Подготовлено для PostgreSQL 15+</span>
    </div>
  </div>
<script>{js}</script>
</body>
</html>
"""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(html_doc).strip())

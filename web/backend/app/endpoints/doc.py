from __future__ import annotations

import html as _html
import json
import os
import re
import textwrap
from datetime import datetime
from typing import Any, Tuple
from urllib.parse import urlparse, urlunparse

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import Response, HTMLResponse, PlainTextResponse
from sqlalchemy import create_engine

# === ваш код / зависимости ===
from pgqueryguard.outer_database.inspect import run_explain
from pgqueryguard.outer_database.count_resourses import CostProfile, estimate_profile
from pgqueryguard.outer_database.advice import Advice
from app.utils.llm.query_improve import improve_and_filter_sql

api_router = APIRouter(prefix="/doc", tags=["doc"])

# ----------------------------- SQL file parsing ------------------------------
_SQL_COMMENT_RE = re.compile(r"--.*?$|/\*.*?\*/", re.S | re.M)

def _read_sql_bytes(data: bytes, size_limit: int = 1_000_000) -> str:
    if len(data) > size_limit:
        raise HTTPException(status_code=413, detail=f"SQL файл слишком большой (> {size_limit} байт)")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("cp1251", errors="strict")
    text = _SQL_COMMENT_RE.sub(" ", text).strip()
    for part in (p.strip() for p in text.split(";")):
        if part:
            return part
    return ""

# добавим поддержку старого префикса и перепишем его
_ALLOWED_SCHEMES = {
    "postgresql", "postgres",
    "postgresql+psycopg",     # psycopg3
    "postgresql+psycopg2",    # допустим ввод и перепишем
}

def _normalize_dsn(raw: str) -> str:
    if not raw:
        raise HTTPException(status_code=400, detail="DSN пустой")

    parsed = urlparse(raw)

    # допускаем psycopg2 и сразу переписываем на psycopg3
    scheme = parsed.scheme
    if scheme not in _ALLOWED_SCHEMES:
        raise HTTPException(status_code=400, detail="Поддерживаются только DSN c postgres/postgresql схемой")

    if not parsed.hostname or not parsed.path or parsed.path in ("/", ""):
        raise HTTPException(status_code=400, detail="В DSN должен быть host и имя базы")

    if scheme in {"postgres", "postgresql", "postgresql+psycopg2"}:
        scheme = "postgresql+psycopg"

    return urlunparse(parsed._replace(scheme=scheme))


def _dsn_label(dsn: str) -> str:
    """
    Для подписи в отчёте: host[:port]/dbname
    """
    p = urlparse(dsn)
    host = p.hostname or "localhost"
    port = f":{p.port}" if p.port else ""
    db = (p.path or "/").lstrip("/")
    return f"{host}{port}/{db}"

def _default_dsn() -> str:
    # можно задать в окружении, чтобы сразу было префилл в форме
    env = os.getenv("DB_DSN", "").strip()
    return env or "postgresql://user:password@localhost:5432/dbname"

# ----------------------------- number formatting -----------------------------
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
    return _html.escape(str(s), quote=True)

def _node_kv(n: dict[str, Any], keys: list[str]) -> str:
    parts = []
    for k in keys:
        v = n.get(k)
        if v is not None:
            parts.append(f'<span class="kv"><b>{_escape(k)}:</b> {_escape(v)}</span>')
    return " · ".join(parts)

# ----------------------------- EXPLAIN rendering -----------------------------
def plan_to_tree_html(plan_json: dict[str, Any]) -> str:
    p = plan_json.get("Plan", plan_json)
    def render(n: dict[str, Any]) -> str:
        title = n.get("Node Type", "Node")
        header = _escape(title)
        meta = _node_kv(n, ["Relation Name", "Index Name", "Join Type", "Parallel Aware"])
        costs = _node_kv(n, ["Startup Cost", "Total Cost", "Plan Rows", "Plan Width"])
        keys = []
        if n.get("Sort Key"):
            keys.append(f'<span class="kv"><b>Sort Key:</b> {_escape(n["Sort Key"])}</span>')
        if n.get("Hash Cond"):
            keys.append(f'<span class="kv"><b>Hash Cond:</b> {_escape(n["Hash Cond"])}</span>')
        if n.get("Merge Cond"):
            keys.append(f'<span class="kv"><b>Merge Cond:</b> {_escape(n["Merge Cond"])}</span>')
        keys_html = " · ".join(keys)
        filt = n.get("Filter")
        filt_html = f'<div class="filter">Filter: <code>{_escape(filt)}</code></div>' if filt else ""
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
    rows_html: list[str] = []
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
    <tbody>{"".join(rows_html)}</tbody>
</table>
"""

def advice_section(advice: list[Advice]) -> str:
    if not advice:
        return '<p class="muted">Рекомендации не найдены — план выглядит разумно.</p>'
    priority_to_class = {"high": "badge high", "medium": "badge med", "low": "badge low"}
    cards = []
    for a in advice:
        pri = priority_to_class.get(getattr(a, "priority", "low"), "badge")
        ddl_block = ""
        ddl = getattr(a, "ddl", None)
        if ddl:
            code = _escape(ddl.strip())
            ddl_block = f"""
<div class="ddl">
  <div class="ddl-head">DDL предложение</div>
  <pre><code>{code}</code></pre>
  <button class="copy" onclick="copyDDL(this)">Копировать</button>
</div>"""
        est_speedup = getattr(a, "est_speedup", None)
        speed = f'<span class="speed">⏩ Ожидаемое ускорение: {_escape(est_speedup)}</span>' if est_speedup else ""
        msg = _escape(getattr(a, "message", ""))
        cards.append(f"""
<article class="card">
  <div class="card-top">
    <span class="{pri}">{_escape(str(getattr(a, "priority", "low")).upper())}</span>
    {speed}
  </div>
  <div class="msg">{msg}</div>
  {ddl_block}
</article>""")
    return '<div class="cards">' + "".join(cards) + "</div>"

def ai_advice_section(ai_advice: list[dict] | None) -> str:
    if not ai_advice:
        return "<div class='muted'>Нет AI-вариантов, прошедших фильтр по EXPLAIN.</div>"

    def fmt_pct_signed(v):
        try:
            return f"{-float(v):+.1f}%"
        except Exception:
            return "—"

    def warn_delta_and_class(v):
        try:
            iv = int(v)
        except Exception:
            return "—", ""
        cls = "low" if iv > 0 else ("high" if iv < 0 else "")
        return f"{iv:+d}", cls

    cards = []
    for i, cand in enumerate(ai_advice, 1):
        sql = (cand.get("sql") or "").strip()
        explanation = cand.get("explanation") or ""
        changes = cand.get("changes") or []
        tags = cand.get("tags") or []

        imp = cand.get("improvement") or {}
        cost_pct   = imp.get("cost_pct");   pages_pct  = imp.get("pages_pct")
        memory_pct = imp.get("memory_pct"); rows_pct   = imp.get("rows_pct")
        warn_diff  = imp.get("warnings_diff", 0); score = imp.get("weighted_geom_ratio")

        c_cost = cand.get("c_cost"); c_pages = cand.get("c_pages")
        c_mem  = cand.get("c_mem");  c_rows  = cand.get("c_rows")
        c_warn = cand.get("c_warnings")

        tags_html = "".join(f"<span class='badge'>{_escape(str(t))}</span>" for t in tags)
        warn_delta_str, warn_cls = warn_delta_and_class(warn_diff)

        chips = f"""
        <div class="chips">
          <span class="badge">Cost: {fmt_float(c_cost)} ({fmt_pct_signed(cost_pct)})</span>
          <span class="badge">Pages: {fmt_num(c_pages)} ({fmt_pct_signed(pages_pct)})</span>
          <span class="badge">Memory: {fmt_bytes(c_mem)} ({fmt_pct_signed(memory_pct)})</span>
          <span class="badge">Rows: {fmt_num(c_rows)} ({fmt_pct_signed(rows_pct)})</span>
          <span class="badge {warn_cls}">Warnings Δ {warn_delta_str}</span>
          <span class="badge">Score: {fmt_float(score)}</span>
        </div>
        """

        changes_html = ""
        if changes:
            items = "".join(f"<li>{_escape(str(it))}</li>" for it in changes)
            changes_html = f"<div class='ddl-head'>Изменения</div><ul class='warn-list'>{items}</ul>"

        cards.append(f"""
        <div class="card">
          <div class="card-top">
            <div><b>Вариант {i}</b></div>
            <div class="chips">{tags_html}</div>
          </div>
          <div class="msg">{_escape(explanation)}</div>
          {chips}
          <div class="ddl-head">SQL (улучшенный)</div>
          <pre class="sql"><code>{_escape(sql)}</code></pre>
          <button class="copy" onclick="copyDDL(this)">Копировать</button>
          {changes_html}
        </div>
        """)

    return f"<div class='cards'>{''.join(cards)}</div>"

# --------------------------- Report HTML builder -----------------------------
def build_html_report(
    plan_json: dict[str, Any],
    profile: CostProfile,
    advice: list[Advice] | None,
    ai_advice: list[dict] | None,
    sql_text: str | None = None,
    db_dsn_label: str | None = None,
) -> str:
    risk_lvl, risk_note = risk_from_profile(profile)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql_block = f'<pre class="sql"><code>{_escape(sql_text.strip())}</code></pre>' if sql_text else ""
    warnings_html = "".join(f"<li>{_escape(w)}</li>" for w in (profile.warnings or []))
    if warnings_html:
        warnings_html = f'<ul class="warn-list">{warnings_html}</ul>'
    plan_tree = plan_to_tree_html(plan_json)
    nodes_table = plan_nodes_table(plan_json)
    advice_html = advice_section(advice or [])
    plan_raw_json = _html.escape(json.dumps(plan_json, ensure_ascii=False, indent=2))
    ai_html = ai_advice_section(ai_advice) if ai_advice else ""
    ai_section = f"<div class='section'><h3>AI рекомендации</h3>{ai_html}</div>" if ai_html else ""

    css = """/* (тот же CSS, что и раньше) */"""  # ← оставьте ваш CSS из предыдущей версии
    js = """
function copyDDL(btn){
  const code = btn.previousElementSibling?.querySelector('code');
  if(!code) return;
  const text = code.innerText;
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
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SQL Advisor Report</title>
  <style>{css}</style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="h-title">SQL Advisor — отчёт анализа запроса</div>
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

    {ai_section}

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
      <h3>EXPLAIN (FORMAT JSON) — исходник</h3>
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
    return textwrap.dedent(html_doc).strip()

# ------------------------------- UI (form) -----------------------------------
@api_router.get("/", response_class=HTMLResponse)
def doc_index() -> str:
    default_dsn = _escape(_default_dsn())
    return f"""
    <html><head><title>SQL Optimizer</title></head>
    <body style="font-family:system-ui,Segoe UI,Arial;margin:24px;max-width:920px">
      <h1>SQL Optimizer</h1>
      <form method="post" action="/doc/report/upload" enctype="multipart/form-data">
        <div style="margin-bottom:8px">
          <label>PostgreSQL DSN:</label><br/>
          <input name="dsn" type="text" style="width:100%" value="{default_dsn}"
                 placeholder="postgresql://user:pass@host:5432/dbname" required />
        </div>
        <div style="margin-bottom:8px">
          <label>Вариантов:</label>
          <input type="number" name="n_variants" value="5" min="1" max="10"/>
        </div>
        <div style="margin-bottom:8px">
          <label>SQL файл:</label><br/>
          <input type="file" name="file" accept=".sql" required>
        </div>
        <button type="submit">Загрузить и оптимизировать</button>
      </form>
    </body></html>
    """

# ------------------------------- Main route ----------------------------------
@api_router.post("/report/upload", response_class=Response)
async def doc_report_upload(
    file: UploadFile = File(...),
    dsn: str = Form(...),
    n_variants: int = Form(5),
) -> Response:
    if not file.filename.lower().endswith(".sql"):
        raise HTTPException(status_code=400, detail="Ожидается .sql файл")

    sql = _read_sql_bytes(await file.read())
    if not sql:
        raise HTTPException(status_code=400, detail="В файле не найден валидный SQL")

    # нормализуем DSN и делаем подпись
    normalized_dsn = _normalize_dsn(dsn)
    label = _dsn_label(normalized_dsn)

    engine = create_engine(normalized_dsn, pool_pre_ping=True)

    # 1) EXPLAIN + профиль
    try:
        plan = run_explain(engine, sql)
        profile = estimate_profile(plan)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"EXPLAIN/estimate ошибка: {e}")

    # 2) AI-варианты
    try:
        variants = await improve_and_filter_sql(
            engine,
            sql,
            profile=profile,
            n_variants=int(n_variants),
            dialect="PostgreSQL 15",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM pipeline ошибка: {e}")

    # 3) Доменные советы (опционально)
    advice: list[Advice] = []

    # 4) HTML → скачать
    html_report = build_html_report(
        plan_json=plan,
        profile=profile,
        advice=advice,
        ai_advice=variants,
        sql_text=sql,
        db_dsn_label=label,
    )
    return Response(
        content=html_report,
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="report.html"'},
    )

@api_router.get("/health", response_class=PlainTextResponse, include_in_schema=False)
def health() -> str:
    return "ok"

import html
import json
import os
from dataclasses import asdict, dataclass


@dataclass
class IndexItem:
    title: str
    file: str
    report_rel: str
    risk: str
    total_cost: float
    est_pages: float
    est_bytes: float
    warnings: int
    excerpt: str
    error: str | None = None


def write_index_page(
    output_dir: str,
    items: list[IndexItem],
    title: str = "SQL Advisor — отчёты",
    manifest: bool = True,
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    index_path = os.path.join(output_dir, "index.html")

    data_js = json.dumps([asdict(i) for i in items], ensure_ascii=False)

    css = """
:root{--bg:#0b1020;--fg:#E7ECF4;--muted:#9AA6B2;--card:#121a33;--border:#22305b;--chip:#1b2447;--high:#ff6b6b;--med:#f2cc60;--ok:#3fb950}
@media (prefers-color-scheme: light){:root{--bg:#f7f9fc;--fg:#0c1220;--muted:#697586;--card:#ffffff;--border:#e5e9f2;--chip:#eef2ff}}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial}
.container{max-width:1200px;margin:0 auto;padding:16px}
.header{padding:8px 0 14px;border-bottom:1px solid var(--border);display:flex;gap:10px;align-items:center;justify-content:space-between}
.h1{font-size:18px;font-weight:700}
.controls{display:flex;gap:8px;margin:12px 0;flex-wrap:wrap}
input[type=search],select{padding:8px 10px;border:1px solid var(--border);border-radius:10px;background:var(--chip);color:var(--fg)}
select{max-width:140px}
.table{width:100%;border-collapse:collapse;border:1px solid var(--border);border-radius:12px;overflow:hidden}
.table tr{cursor:pointer}
.table tr:hover{background:rgba(124,155,255,.08)}
.table th,.table td{border-bottom:1px solid var(--border);padding:10px}
.table td.num{text-align:right}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;border:1px solid var(--border);font-size:12px}
.low{background:rgba(63,185,80,.12);color:var(--ok);border-color:rgba(63,185,80,.35)}
.med{background:rgba(242,204,96,.12);color:var(--med);border-color:rgba(242,204,96,.35)}
.high{background:rgba(255,107,107,.12);color:var(--high);border-color:rgba(255,107,107,.35)}
.err{background:#3f1d1d;color:#ffb4b4;border-color:#6b1d1d}
.excerpt{color:var(--muted);font-size:12px;white-space:nowrap;text-overflow:ellipsis;overflow:hidden;max-width:520px}
a.rowlink{color:inherit;text-decoration:none}
    """

    js = (
        """
const DATA = %s;
let sortKey = "title", sortDir = 1;
function riskClass(r){return r==="HIGH"?"high":r==="MED"?"med":r==="LOW"?"low":"err"}
function esc(s){return (s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")}
function openReport(rel){ window.location.href = rel; } // переход в той же вкладке
function setSort(k){ if(sortKey===k) sortDir*=-1; else {sortKey=k; sortDir=1;} render(); }
function render(){
  const q = document.querySelector("#q").value.toLowerCase();
  const rf = document.querySelector("#risk").value;
  const tbody = document.querySelector("#tbody");
  let rows = DATA.filter(x => 
    (rf==="ALL" || x.risk===rf) &&
    (x.title.toLowerCase().includes(q) || x.file.toLowerCase().includes(q) || (x.excerpt||"").toLowerCase().includes(q))
  );
  rows.sort((a,b)=>{
    let va=a[sortKey], vb=b[sortKey];
    if(typeof va==="string") va=va.toLowerCase();
    if(typeof vb==="string") vb=vb.toLowerCase();
    return (va>vb?1:va<vb?-1:0)*sortDir;
  });
  tbody.innerHTML = rows.map(x=>`
    <tr onclick="openReport('${x.report_rel}')">
      <td>
        <a class="rowlink" href="${x.report_rel}">${esc(x.title)}</a>
        <div class="excerpt" title="${esc(x.file)}">${esc(x.excerpt)}</div>
      </td>
      <td><span class="badge ${riskClass(x.risk)}">${x.risk}</span></td>
      <td class="num">${(x.total_cost||0).toFixed(2)}</td>
      <td class="num">${Math.round(x.est_pages||0).toLocaleString()}</td>
      <td class="num">${Math.round((x.est_bytes||0)/1024/1024)} MB</td>
      <td class="num">${x.warnings||0}</td>
    </tr>`).join("");
}
window.addEventListener("DOMContentLoaded", render);
"""
        % data_js
    )

    html_doc = f"""<!doctype html>
<meta charset="utf-8"><title>{html.escape(title)}</title>
<style>{css}</style>
<div class="container">
  <div class="header">
    <div class="h1">{html.escape(title)}</div>
    <div class="controls">
      <input id="q" type="search" placeholder="Поиск по имени/пути/SQL..." oninput="render()">
      <select id="risk" onchange="render()">
        <option value="ALL">Все риски</option>
        <option value="HIGH">HIGH</option>
        <option value="MED">MED</option>
        <option value="LOW">LOW</option>
        <option value="ERROR">ERROR</option>
      </select>
    </div>
  </div>

  <table class="table">
    <thead>
      <tr>
        <th onclick="setSort('title')">Файл</th>
        <th onclick="setSort('risk')">Риск</th>
        <th onclick="setSort('total_cost')">Cost</th>
        <th onclick="setSort('est_pages')">Страницы</th>
        <th onclick="setSort('est_bytes')">Данные</th>
        <th onclick="setSort('warnings')">Warn</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>
<script>{js}</script>
"""

    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    if manifest:
        with open(
            os.path.join(output_dir, "manifest.json"), "w", encoding="utf-8"
        ) as f:
            json.dump([asdict(i) for i in items], f, ensure_ascii=False, indent=2)

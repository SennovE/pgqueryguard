from __future__ import annotations
from fastapi.responses import HTMLResponse, PlainTextResponse, Response

from fastapi import APIRouter, HTTPException, Form, UploadFile, File
from fastapi.responses import HTMLResponse, PlainTextResponse
from sqlalchemy import create_engine

from pgqueryguard.outer_database.inspect import run_explain
from pgqueryguard.outer_database.count_resourses import estimate_profile
from app.utils.llm.query_improve import improve_and_filter_sql

import io
import re

api_router = APIRouter(prefix="/doc", tags=["doc"]) 

_SQL_COMMENT_RE = re.compile(r"--.*?$|/\*.*?\*/", re.S | re.M)


def _read_sql_bytes(data: bytes, size_limit: int = 1_000_000) -> str:
    """Decode bytes to str (utf-8 with fallback), trim, drop comments.

    Limits total size to size_limit. Returns a single SQL statement string.
    If multiple statements found, keeps the first non-empty.
    """
    if len(data) > size_limit:
        raise HTTPException(status_code=413, detail=f"SQL файл слишком большой (> {size_limit} байт)")

    # Try utf-8 then cp1251 as fallback for RU users
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("cp1251", errors="strict")

    # Drop comments and trim
    text = _SQL_COMMENT_RE.sub(" ", text)
    text = text.strip()

    # Split on semicolons, take first non-empty
    parts = [p.strip() for p in text.split(";")]
    for p in parts:
        if p:
            return p
    return ""


# ---------------------------------------------------------------------
# HTML upload form (convenient UI in the browser)
# ---------------------------------------------------------------------
@api_router.get("/", response_class=HTMLResponse)
def doc_index():
    return """
    <html><head><title>SQL Optimizer</title>
      <style>
        body{font-family:system-ui,Segoe UI,Arial;margin:24px;max-width:920px}
        textarea{width:100%;height:160px}
        pre{background:#f6f8fa;padding:12px;border-radius:8px;overflow:auto}
        .cand{border:1px solid #e5e7eb;border-radius:10px;padding:12px;margin:12px 0}
        .metrics{font-size:0.95em;color:#374151}
        .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
        input[type=number]{width:80px}
      </style>
    </head>
    <body>
      <h1>SQL Optimizer</h1>

      <h3>Вставить SQL</h3>
      <form method="post" action="/doc/report">
        <label>SQL:</label><br/>
        <textarea name="sql" placeholder="SELECT ..."></textarea><br/><br/>
        <div class="row">
          <label>Число вариантов:</label>
          <input type="number" name="n_variants" value="5" min="1" max="10"/>
          <button type="submit">Оптимизировать</button>
        </div>
      </form>

      <hr/>

      <h3>Загрузить .sql файл</h3>
      <form method="post" action="/doc/report/upload" enctype="multipart/form-data">
        <input type="file" name="file" accept=".sql" required>
        <input type="number" name="n_variants" value="5" min="1" max="10"/>
        <button type="submit">Загрузить и оптимизировать</button>
      </form>
    </body></html>
    """

@api_router.post("/report", response_class=HTMLResponse)
async def doc_report(sql: str = Form(...), n_variants: int = Form(5)):
    sql = (sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL пустой")
    return await _process_sql_and_render(sql, n_variants)



@api_router.post("/report/upload", response_class=HTMLResponse)
async def doc_report_upload(
    file: UploadFile = File(...),
    n_variants: int = Form(5),
):
    if not file.filename.lower().endswith(".sql"):
        raise HTTPException(status_code=400, detail="Ожидается .sql файл")

    raw = await file.read()
    sql = _read_sql_bytes(raw)
    if not sql:
        raise HTTPException(status_code=400, detail="В файле не найден валидный SQL")

    return await _process_sql_and_render(sql, n_variants)


from fastapi.responses import HTMLResponse, PlainTextResponse, Response

def downloadable_html(html: str, filename: str = "report.html") -> Response:
    return Response(
        content=html,
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

async def _process_sql_and_render(sql: str, n_variants: int) -> Response:
    engine = create_engine(
        "postgresql+psycopg://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs",
        pool_pre_ping=True,
    )
    try:
        plan = run_explain(engine, sql)
        profile = estimate_profile(plan)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"EXPLAIN/estimate ошибка: {e}")

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

    blocks: list[str] = []
    for i, v in enumerate(variants or [], 1):
        m = v.get("improvement", {}) if isinstance(v, dict) else {}
        sql_txt = (v.get("sql") or "") if isinstance(v, dict) else ""
        sql_txt = sql_txt.replace("<", "&lt;").replace(">", "&gt;")
        blocks.append(f"""
        <div class="cand">
          <h3>Вариант {i}</h3>
          <pre>{sql_txt}</pre>
          <div class="metrics">
            <div><b>Пояснение:</b> {v.get('explanation','') if isinstance(v, dict) else ''}</div>
            <div>
              <b>Метрики:</b>
              cost {m.get('cost_pct',0):.1f}% | pages {m.get('pages_pct',0):.1f}% |
              memory {m.get('memory_pct',0):.1f}% | rows {m.get('rows_pct',0):.1f}% |
              warnings Δ {m.get('warnings_diff',0):+d} | score={m.get('weighted_geom_ratio',1.0):.3f}
            </div>
            <div>
              [cost={v.get('c_cost',0):.2f}, pages={v.get('c_pages',0):.1f},
               mem={v.get('c_mem',0):.0f}, rows={v.get('c_rows',0):.0f},
               warnings={v.get('c_warnings',0)}]
            </div>
          </div>
        </div>
        """)

    if not blocks:
        html_out = """
        <html><body>
          <a href="/doc/">← Назад</a>
          <p>Нет кандидатов, которые по EXPLAIN выглядят лучше базового.</p>
        </body></html>
        """
    else:
        html_out = f"""
        <html><head><title>Отчёт</title></head>
        <body>
          <a href="/doc/">← Назад</a>
          <h2>Базовый SQL</h2>
          <pre>{sql.replace('<','&lt;').replace('>','&gt;')}</pre>
          <h2>Кандидаты</h2>
          {''.join(blocks)}
        </body></html>
        """

    return downloadable_html(html_out, filename="report.html")


@api_router.get("/health", response_class=PlainTextResponse, include_in_schema=False)
def health() -> str:
    return "ok"

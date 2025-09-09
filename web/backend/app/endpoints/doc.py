from fastapi import APIRouter, HTTPException, Form
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine

from app.ulits.checker.src.pgqueryguard.outer_database.inspect import run_explain
from app.ulits.checker.src.pgqueryguard.outer_database.count_resourses import estimate_profile
from app.ulits.llm.query_improve import improve_and_filter_sql 

api_router = APIRouter(prefix="/doc", tags=["doc"])

# Форма (HTML)
@api_router.get("", response_class=HTMLResponse)
def doc_index():
    return """
    <html><head><title>SQL Optimizer</title>
      <style>
        body{font-family:system-ui,Segoe UI,Arial;margin:24px;max-width:900px}
        textarea{width:100%;height:160px}
        pre{background:#f6f8fa;padding:12px;border-radius:8px;overflow:auto}
        .cand{border:1px solid #e5e7eb;border-radius:10px;padding:12px;margin:12px 0}
        .metrics{font-size:0.95em;color:#374151}
      </style>
    </head>
    <body>
      <h1>SQL Optimizer</h1>
      <form method="post" action="/doc/report">
        <label>SQL:</label><br/>
        <textarea name="sql" placeholder="SELECT ..."></textarea><br/><br/>
        <label>Число вариантов:</label>
        <input type="number" name="n_variants" value="5" min="1" max="10"/>
        <button type="submit">Оптимизировать</button>
      </form>
    </body></html>
    """

# Генерация HTML-отчёта
@api_router.post("/report", response_class=HTMLResponse)
async def doc_report(
    sql: str = Form(...),
    n_variants: int = Form(5),
):
    sql = (sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL пустой")

    # DSN можешь вынести в .env и брать через свои settings; здесь — прямо.
    engine = create_engine(
        "postgresql+psycopg2://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs",
        pool_pre_ping=True,
    )

    # 1) EXPLAIN и профиль
    try:
        plan = run_explain(engine, sql)
        profile = estimate_profile(plan)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"EXPLAIN/estimate ошибка: {e}")

    # 2) Вызов твоей функции улучшений
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

    if not variants:
        return HTMLResponse("""
        <html><body>
          <a href="/doc">← Назад</a>
          <p>Нет кандидатов, которые по EXPLAIN выглядят лучше базового.</p>
        </body></html>
        """)

    # 3) HTML
    blocks = []
    for i, v in enumerate(variants, 1):
        m = v.get("improvement", {})
        blocks.append(f"""
        <div class="cand">
          <h3>Вариант {i}</h3>
          <pre>{(v.get("sql") or "").replace("<","&lt;").replace(">","&gt;")}</pre>
          <div class="metrics">
            <div><b>Пояснение:</b> {v.get("explanation","")}</div>
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

    return HTMLResponse(f"""
    <html><head><title>Отчёт</title></head>
    <body>
      <a href="/doc">← Назад</a>
      <h2>Базовый SQL</h2>
      <pre>{sql.replace("<","&lt;").replace(">","&gt;")}</pre>
      <h2>Кандидаты</h2>
      {''.join(blocks)}
    </body></html>
    """)

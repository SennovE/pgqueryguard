import asyncio
from sqlalchemy import create_engine


from pgqueryguard.llm.query_improve import improve_and_filter_sql
from pgqueryguard.outer_database.count_resourses import CostProfile, estimate_profile
from pgqueryguard.outer_database.inspect import run_explain


if __name__ == "__main__":
    engine = create_engine("postgresql+psycopg2://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs")

    async def demo():
        baseline_sql = """
        SELECT * FROM "rnacen"."rna" WHERE crc64 LIKE '%A';
        """

        plan = run_explain(engine, baseline_sql)
        profile = estimate_profile(plan)

        print(1)

        variants = await improve_and_filter_sql(
            engine,
            baseline_sql,
            profile=profile,
            n_variants=5,
            dialect="PostgreSQL 15",
            # при желании можно выбрать другой провайдер:
            # api_url="https://your-compatible-endpoint/v1/chat/completions",
            # model="your-model",
            # work_mem_bytes=128*1024*1024,
        )
        if not variants:
            print("Нет кандидатов, которые по EXPLAIN выглядят лучше базового.")
            return

        for i, cand in enumerate(variants, 1):
            print(f"\n=== Variant {i} ===")
            print(cand["sql"])
            print("—", cand.get("explanation", ""))
            m = cand["improvement"]
            print(f"Improvement: cost {m['cost_pct']:.1f}% | pages {m['pages_pct']:.1f}% | "
                  f"memory {m['memory_pct']:.1f}% | rows {m['rows_pct']:.1f}% | "
                  f"warnings Δ {m['warnings_diff']:+d} | score={m['weighted_geom_ratio']:.3f}")
            print(f"[metrics] cost={cand['c_cost']:.2f}, pages={cand['c_pages']:.1f}, "
                  f"mem={cand['c_mem']:.0f}, rows={cand['c_rows']:.0f}, warnings={cand['c_warnings']}")

    asyncio.run(demo())
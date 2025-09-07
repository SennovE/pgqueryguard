from dataclasses import dataclass
from typing import Any


@dataclass
class Advice:
    priority: str
    message: str
    ddl: str | None = None
    est_speedup: str | None = None


def advise_from_plan(
    plan_json: dict[str, Any], tables_stats: dict[str, dict[str, Any]]
) -> list[Advice]:
    adv: list[Advice] = []
    p = plan_json["Plan"]

    def walk(n: dict[str, Any], parent: dict[str, Any] | None = None):
        nt = n.get("Node Type", "")
        rel = n.get("Relation Name")
        filt = n.get("Filter")
        sortk = n.get("Sort Key")
        join_type = n.get("Join Type")

        if nt == "Seq Scan" and rel and filt:
            info = tables_stats.get(rel, {})
            relpages = info.get("relpages", 0)
            if relpages > 10_000:
                cols = []
                for token in (
                    str(filt)
                    .replace("(", " ")
                    .replace(")", " ")
                    .replace("::", " ")
                    .replace("'", " ")
                    .split()
                ):
                    if "." in token and token.isidentifier():
                        cols.append(token.split(".")[-1])
                cols = [c for c in cols if c.isidentifier()]
                cols = cols[:3] or ["<column>"]
                ddl = f'CREATE INDEX ON "{rel}" ({", ".join(cols)});'
                adv.append(
                    Advice(
                        "high",
                        f"{rel}: big seq scan with filter {filt} - try to use index.",
                        ddl=ddl,
                        est_speedup="3-20x",
                    )
                )

        if nt == "Sort" and sortk and parent and parent.get("Relation Name"):
            rel2 = parent["Relation Name"]
            cols = [k.split()[-1].strip('"') for k in sortk]
            ddl = f'CREATE INDEX ON "{rel2}" ({", ".join(cols)});'
            adv.append(
                Advice(
                    "medium",
                    f"{rel2}: expensive sort on {sortk} - try to use index.",
                    ddl=ddl,
                )
            )

        if nt == "Nested Loop" and join_type in (None, "Inner"):
            pass

        for ch in n.get("Plans") or []:
            walk(ch, parent=n)

    walk(p, None)
    return adv

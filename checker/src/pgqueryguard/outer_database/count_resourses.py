from dataclasses import dataclass
from typing import Any


@dataclass
class CostProfile:
    total_cost: float
    est_rows: float
    est_bytes: float
    est_pages: float
    est_memory_bytes: float
    nodes: list[tuple[str, float]]
    warnings: list[str]


PAGE = 8192


def estimate_profile(
    plan_json: dict[str, Any], work_mem_bytes: int = 64 * 1024 * 1024
) -> CostProfile:
    p = plan_json["Plan"]
    warnings: list[str] = []

    def walk(n: dict[str, Any]) -> dict[str, float]:
        rows = float(n.get("Plan Rows", 0))
        width = float(n.get("Plan Width", 0))
        node_bytes = rows * width
        node_pages = node_bytes / PAGE
        mem_need = 0.0
        nt = n.get("Node Type", "")
        if nt == "Sort":
            mem_need = node_bytes * 1.2
            if mem_need > work_mem_bytes:
                warnings.append(
                    f"Maybe need spill for Sort (~{int(mem_need / 1e6)} MB > work_mem)"
                )
        elif nt.startswith("Hash"):
            mem_need = node_bytes * 1.3
            if mem_need > work_mem_bytes:
                warnings.append(
                    f"Maybe need spill for Hash (~{int(mem_need / 1e6)} MB > work_mem)"
                )
        acc = {"rows": rows, "bytes": node_bytes, "pages": node_pages, "mem": mem_need}
        for ch in n.get("Plans") or []:
            c = walk(ch)
            for k in acc:
                acc[k] += c[k]
        return acc

    acc = walk(p)
    total_cost = float(p.get("Total Cost") or p.get("Plan Rows", 0))

    nodes = []

    def collect(n):
        nodes.append((n.get("Node Type", ""), float(n.get("Plan Rows", 0))))
        for ch in n.get("Plans") or []:
            collect(ch)

    collect(p)

    return CostProfile(
        total_cost=total_cost,
        est_rows=acc["rows"],
        est_bytes=acc["bytes"],
        est_pages=acc["pages"],
        est_memory_bytes=acc["mem"],
        nodes=nodes,
        warnings=warnings,
    )

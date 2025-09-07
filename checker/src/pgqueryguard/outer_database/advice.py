from dataclasses import dataclass
from typing import Any, Optional
import re


@dataclass
class Advice:
    priority: str
    message: str
    ddl: Optional[str] = None
    est_speedup: Optional[str] = None
    index_type: Optional[str] = None


IDENT = r'"[^"]+"|[A-Za-z_][A-Za-z0-9_]*'
COLREF_RE = re.compile(rf"({IDENT})\s*\.\s*({IDENT})")
SINGLECOL_RE = re.compile(
    rf"(?<!\.)\b({IDENT})\b\s*(=|<>|!=|<|>|<=|>=|~~\*?|!~~\*?|LIKE|ILIKE|BETWEEN|IN\s*\(|@>|<@|\?|\?\||\?&|&&|@@)",
    re.IGNORECASE,
)
LITERAL_RE = re.compile(r"'([^']*)'")


def _unquote(ident: str) -> str:
    ident = ident.strip()
    if ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1]
    return ident


def _uniq_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def extract_cols_from_filter(filt: str) -> list[str]:
    cols: list[str] = []
    if not filt:
        return cols
    for m in COLREF_RE.finditer(filt):
        cols.append(_unquote(m.group(2)))
    for m in SINGLECOL_RE.finditer(filt):
        cols.append(_unquote(m.group(1)))
    return _uniq_keep_order(cols)


def extract_cols_from_sortkey(sortk: list[str]) -> list[str]:
    cols: list[str] = []
    for k in sortk:
        m = COLREF_RE.search(k)
        if m:
            cols.append(_unquote(m.group(2)))
            continue
        last_ident = re.findall(IDENT, k)
        if last_ident:
            cols.append(_unquote(last_ident[-1]))
    return _uniq_keep_order(cols)


def is_trigram_like(filter_str: str) -> bool:
    s = filter_str or ""
    if not any(tok in s.upper() for tok in ("LIKE", "ILIKE")) and "~~" not in s:
        return False

    for lit in LITERAL_RE.findall(s):
        if "%" in lit:
            if lit.startswith("%") or ("%" in lit[:-1]):
                return True
    return False


def has_json_array_ops(s: str) -> bool:
    s = f" {s or ''} "
    return any(op in s for op in (" @> ", " <@ ", " ? ", " ?| ", " ?& ", " && "))


def has_fulltext(s: str) -> bool:
    return "@@" in (s or "")


def has_range_cmp(s: str) -> bool:
    s = f" {s or ''} "
    return any(op in s for op in (" < ", " > ", " <= ", " >= ", " BETWEEN "))


def pick_index_type(
    filt: Optional[str], sortk: Optional[list[str]], relpages: int, cols: list[str]
) -> tuple[str, str, Optional[str]]:
    if sortk:
        cols2 = extract_cols_from_sortkey(sortk)
        if not cols2:
            cols2 = cols or ["<column>"]
        ddl_cols = ", ".join(f'"{c}"' for c in cols2)
        return "btree", ddl_cols, None

    s = filt or ""
    cols = cols or ["<column>"]

    if is_trigram_like(s):
        c = f'"{cols[0]}"'
        return "gin_trgm", f"{c} gin_trgm_ops", "нужно расширение pg_trgm"

    if has_json_array_ops(s) or has_fulltext(s):
        ddl_cols = ", ".join(f'"{c}"' for c in cols[:3])
        return "gin", ddl_cols, None

    if has_range_cmp(s) and relpages >= 1_000_000:
        ddl_cols = ", ".join(f'"{c}"' for c in cols[:3])
        return "brin", ddl_cols, None

    ddl_cols = ", ".join(f'"{c}"' for c in cols[:3])
    return "btree", ddl_cols, None


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
                cols = extract_cols_from_filter(str(filt))
                idx_type, ddl_cols, note = pick_index_type(
                    str(filt), None, relpages, cols
                )
                using = (
                    "USING gin"
                    if idx_type in ("gin", "gin_trgm")
                    else f"USING {idx_type}"
                )
                ddl = f'CREATE INDEX ON "{rel}" {using} ({ddl_cols});'
                note_str = f" ({note})" if note else ""
                adv.append(
                    Advice(
                        "high",
                        f"{rel}: большой seq scan с фильтром {filt} — попробуйте индекс {idx_type}{note_str}.",
                        ddl=ddl,
                        est_speedup="3-20x",
                        index_type=idx_type,
                    )
                )

        if nt == "Sort" and sortk and parent and parent.get("Relation Name"):
            rel2 = parent["Relation Name"]
            idx_type, ddl_cols, _ = pick_index_type(None, sortk, 0, [])
            ddl = f'CREATE INDEX ON "{rel2}" USING {idx_type} ({ddl_cols});'
            adv.append(
                Advice(
                    "medium",
                    f"{rel2}: дорогая сортировка по {sortk} — попробуйте индекс {idx_type}.",
                    ddl=ddl,
                    index_type=idx_type,
                )
            )

        if nt == "Nested Loop" and join_type in (None, "Inner"):
            pass

        for ch in n.get("Plans") or []:
            walk(ch, parent=n)

    walk(p, None)
    return adv

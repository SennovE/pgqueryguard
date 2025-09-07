import os
import json
import asyncio
from typing import Any, Dict, List, Optional
import httpx
import math
from sqlalchemy import Engine
import sys


from checker.src.pgqueryguard.outer_database.count_resourses import CostProfile, estimate_profile
from checker.src.pgqueryguard.outer_database.inspect import run_explain
from app.llm.api_utils import (
    get_api_key, 
    get_api_url,
    _safe_ratio,
    _weighted_geom_ratio,
    _impr_pct,
)


class SqlImproveError(Exception):
    pass


async def improve_sql(
    sql: str,
    *,
    llm: str = "openai",
    n_variants: int = 3,
    dialect: str = "generic (closest to PostgreSQL)",
    temperature: float = 0.7,
    timeout: float = 60.0,
    extra_headers: Optional[Dict[str, str]] = None,
    extra_payload: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Асинхронно улучшает SQL-запрос через совместимый с OpenAI Chat Completions API.

    Возвращает список объектов-кандидатов:
      [{sql, explanation, changes, semantics, assumptions, tags}, ...]

    :param api_url: полный URL до Chat Completions совместимого провайдера
    :param api_key: ключ; по умолчанию берётся из окружения OPENAI_API_KEY
    :param extra_headers: доп. заголовки HTTP (например, {"HTTP-Referer": "..."} )
    :param extra_payload: доп. поля в тело запроса JSON (например, {"top_p": 0.9})
    """
    if not sql or not sql.strip():
        raise SqlImproveError("Пустой SQL на входе.")

    api_key = get_api_key(llm)
    api_url = get_api_url(llm)

    system = (
        "You are a senior database engineer and SQL optimizer. "
        "Given a SQL query, produce N improved alternatives while preserving the original result semantics by default. "
        "Prefer standard SQL; if dialect-specific, call it out explicitly. "
        "Focus on correctness, then performance and readability. "
        "Пиши на русском языке."
        "Return ONLY valid JSON."
    )

    user = f"""
            Дано (диалект: {dialect}).

            Исходный SQL:
            <<<SQL
            {sql}
            SQL>>>

            Сгенерируй ровно {n_variants} улучшенных вариантов.

            Формат ответа — один JSON-объект с ключом "candidates": список объектов:
            - sql: строка, сам улучшенный SQL
            - explanation: 2–5 предложений, что изменилось и почему лучше
            - changes: массив коротких пунктов правок
            - semantics: "preserved" | "narrower" | "broader" (и почему, если не preserved)
            - assumptions: список явных допущений (индексы, размеры таблиц, кардинальности)
            - tags: массив тегов (например: ["performance","readability","standard_sql","postgres"])

            Правила:
            - Не меняй бизнес-смысл без необходимости; если меняешь — пометь в semantics.
            - Никакого текста вне JSON.
            """.strip()

    payload: Dict[str, Any] = {
        "model": "gpt-4.1-mini",
        "temperature": temperature,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    if extra_payload:
        # Позволяет добавлять совместимые с эндпоинтом поля (top_p, seed, stop и т.п.)
        payload.update(extra_payload)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.post(api_url, headers=headers, json=payload)
        except httpx.RequestError as e:
            raise SqlImproveError(f"HTTP ошибка: {e}") from e

    if resp.status_code != 200:
        raise SqlImproveError(f"API error {resp.status_code}: {resp.text}")

    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        raise SqlImproveError(f"Некорректный JSON от API: {resp.text[:500]}...") from e

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as e:
        raise SqlImproveError(f"Неожиданная форма ответа API: {data}") from e

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as e:
        raise SqlImproveError(f"Модель вернула невалидный JSON: {content[:500]}...") from e

    candidates = parsed.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise SqlImproveError(f"В ответе нет candidates: {parsed}")

    return candidates


async def improve_and_filter_sql(
    engine: Engine,
    baseline_sql: str,
    *,
    profile: CostProfile,
    llm: str = "openai",
    n_variants: int = 3,
    dialect: str = "PostgreSQL 15",
    temperature: float = 0.7,
    extra_headers: Optional[Dict[str, str]] = None,
    extra_payload: Optional[Dict[str, Any]] = None,
    # оценки/пороги:
    work_mem_bytes: int = 64 * 1024 * 1024,
    min_cost_improvement: float = 0.10,      # ≥10% по Total Cost
    min_weighted_improvement: float = 0.15,  # ≥15% по взвешенной геометрии
    warn_relax_cost_drop: float = 0.20,      # если варнингов стало больше — требуем ≥20% по cost
    weights: Optional[Dict[str, float]] = None,  # веса метрик для геометрии
    # Поведение:
    require_preserved_semantics: bool = True,
) -> Dict[str, Any]:
    """
    Возвращает: список объектов-кандидатов ровно в том же формате, что и improve_sql,
    но отфильтрованный по EXPLAIN (без ANALYZE).
    """

    candidates = await improve_sql(
        baseline_sql,
        llm=llm,
        n_variants=n_variants,
        dialect=dialect,
        temperature=temperature,
        extra_headers=extra_headers,
        extra_payload=extra_payload,
    )

    base_cost = float(getattr(profile, "total_cost", 0.0))
    base_pages = float(getattr(profile, "est_pages", 0.0))
    base_mem = float(getattr(profile, "est_memory_bytes", 0.0))
    base_rows = float(getattr(profile, "est_rows", 0.0))
    base_warnings = len(getattr(profile, "warnings", []) or [])

    weights = weights or {"cost": 0.6, "pages": 0.2, "memory": 0.15, "rows": 0.05}
    threshold_ratio = 1.0 - float(min_weighted_improvement)

    shortlisted: List[Dict[str, Any]] = []

    for cand in candidates:
        csql = (cand.get("sql") or "").strip()
        semantics = (cand.get("semantics") or "").strip().lower()

        if not csql:
            continue
        if require_preserved_semantics and semantics and semantics != "preserved":
            continue

        try:
            c_plan = run_explain(engine, csql)
            c_prof = estimate_profile(c_plan, work_mem_bytes)
        except Exception:
            continue

        c_cost = float(getattr(c_prof, "total_cost", 0.0))
        c_pages = float(getattr(c_prof, "est_pages", 0.0))
        c_mem = float(getattr(c_prof, "est_memory_bytes", 0.0))
        c_rows = float(getattr(c_prof, "est_rows", 0.0))
        c_warnings = len(getattr(c_prof, "warnings", []) or [])

        ratios = {
            "cost": _safe_ratio(c_cost, base_cost),
            "pages": _safe_ratio(c_pages, base_pages),
            "memory": _safe_ratio(c_mem, base_mem),
            "rows": _safe_ratio(c_rows, base_rows),
        }
        geom_ratio = _weighted_geom_ratio(ratios, weights)

        cost_better = (base_cost > 0) and ((base_cost - c_cost) / base_cost >= min_cost_improvement)
        weighted_better = geom_ratio <= threshold_ratio

        if c_warnings > base_warnings and not ((base_cost - c_cost) / base_cost >= warn_relax_cost_drop):
            continue

        if cost_better or weighted_better:
            out_cand = dict(cand)
            out_cand.update(
                {
                    "c_cost": c_cost,
                    "c_pages": c_pages,
                    "c_mem": c_mem,
                    "c_rows": c_rows,
                    "c_warnings": c_warnings,
                    "improvement": {
                        "cost_pct": _impr_pct(base_cost, c_cost),
                        "pages_pct": _impr_pct(base_pages, c_pages),
                        "memory_pct": _impr_pct(base_mem, c_mem),
                        "rows_pct": _impr_pct(base_rows, c_rows),
                        "warnings_diff": base_warnings - c_warnings,  # >0 = меньше предупреждений
                        "weighted_geom_ratio": geom_ratio,            # <1.0 = лучше
                    },
                }
            )
            shortlisted.append(out_cand)

    return shortlisted
import os
from app.config import get_settings
from typing import Any, Dict, List, Optional
import math


def get_api_key(llm: str = "openai") -> str:
    s = get_settings()
    match llm:
        case "openai":
            if not s.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is not set")
            return s.OPENAI_API_KEY
        case "deepseek":
            if not s.DEEPSEEK_API_KEY:
                raise ValueError("DEEPSEEK_API_KEY is not set")
            return s.DEEPSEEK_API_KEY
        case _:
            raise ValueError(f"Unknown llm: {llm}")

    

def get_api_url(llm: str = "openai"):
    match llm:
        case "openai":
            return "https://api.openai.com/v1/chat/completions"
        case "deepseek":
            return "https://api.deepseek.com/chat/completions"
        case _:
            raise Exception("Unknown llm")


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return 1.0
    return max(num, 0.0) / den


def _weighted_geom_ratio(
    ratios: Dict[str, float],
    weights: Dict[str, float],
) -> float:
    """
    Взвешенное геометрическое среднее по метрикам в ratios (меньше — лучше).
    ratios: {'cost': 0.8, 'pages': 0.7, ...}
    weights должны суммироваться примерно к 1.0
    """
    s = 0.0
    for k, r in ratios.items():
        w = weights.get(k, 0.0)
        if r <= 0:
            r = 1.0
        s += w * math.log(r)
    return math.exp(s)


def _impr_pct(baseline: float, cand: float) -> float:
    """Сколько процентов улучшения (положительное = лучше)."""
    if baseline <= 0:
        return 0.0
    return max(0.0, (baseline - cand) / baseline * 100.0)
from pathlib import Path
import aiofiles


def bool_(x: str) -> bool:
    return x == "true"


async def parse_opts_for_sqlglot(filename: Path):
    async with aiofiles.open(filename) as f:
        lines = await f.readlines()
        default_types = {
            "identify": bool_,
            "normalize": bool_,
            "normalize_functions": bool_,
            "leading_comma": bool_,
            "comments": bool_,
            "pad": int,
            "indent": int,
            "max_text_width": int,
        }
        opts = {}
        for line in lines:
            name, value = line.strip().replace(" ", "").lower().split("=")
            if name in default_types:
                opts[name] = default_types[name](value)
    return opts

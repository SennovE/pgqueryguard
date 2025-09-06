from logging import getLogger
import asyncio
import os
import sys
import shutil
from pathlib import Path
from asyncio.subprocess import PIPE

import sqlglot

logger = getLogger(__name__)


def format_with_sqlglot(query: str):
    formatted = sqlglot.transpile(query, write="postgres", pretty=True)[0]
    return formatted


async def format_with_pg_formatter(
    sql: str,
    pg_format_bin: Path | None = None,
    opts: list[str] | None = None,
    timeout: float = 10.0,
) -> str:
    opts = opts or []
    cmd = _pg_format_argv(pg_format_bin, opts)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE
        )
    except FileNotFoundError:
        raise RuntimeError(f"Can not find executive file: {cmd[0]}")

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=sql.encode("utf-8")),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"pg_format timeout reach {timeout}s")

    if proc.returncode != 0:
        raise RuntimeError(
            f"pg_format finished with code {proc.returncode}: {stderr.decode(errors='replace')}"
        )

    return stdout.decode("utf-8").removesuffix("\r\n")


def _pg_format_argv(pg_format_bin: Path | None, opts: list[str]) -> list[str]:
    candidate = os.environ.get("PG_FORMAT_BIN") or pg_format_bin or "pg_format"
    p = Path(candidate)

    def argv_with(binary: str) -> list[str]:
        return [binary, *opts, "-"]

    if p.exists():
        if sys.platform.startswith("win") and p.suffix.lower() not in (
            ".exe",
            ".bat",
            ".cmd",
        ):
            perl = shutil.which("perl")
            if not perl:
                raise RuntimeError(
                    "Need to install Perl, if runnig on Windows, "
                    "or use WSL/pg_format.exe."
                )
            return [perl, str(p), *opts, "-"]
        return argv_with(str(p))

    if sys.platform.startswith("win"):
        for name in ("pg_format.exe", "pg_format.bat", "pg_format.cmd", "pg_format"):
            via = shutil.which(name)
            if via:
                return argv_with(via)
        script = shutil.which("pg_format")
        perl = shutil.which("perl")
        if script and perl:
            return [perl, script, *opts, "-"]
        raise FileNotFoundError(
            "Can not find pg_format in PATH. Install it or gevi full path."
        )
    else:
        via = shutil.which(candidate) or shutil.which("pg_format")
        if via:
            return argv_with(via)
        raise FileNotFoundError(
            "Can not find pg_format in PATH. Install it or gevi full path."
        )

from pathlib import Path

import aiofiles


async def read_file(filename: Path) -> str:
    async with aiofiles.open(filename, encoding="utf-8", newline="\n") as file:
        return await file.read()


async def write_file(filename: Path, query: str):
    async with aiofiles.open(filename, "w", encoding="utf-8", newline="\n") as file:
        await file.write(query)


def get_sql_files(path: Path, recursive: bool) -> list[Path]:
    exts = {".sql", ".psql", ".pgsql"}
    if path.is_file():
        return [path] if path.suffix.lower() in exts else []
    it = path.rglob("*") if recursive else path.glob("*")
    return sorted(p for p in it if p.is_file() and p.suffix.lower() in exts)

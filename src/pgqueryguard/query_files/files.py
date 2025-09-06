from pathlib import Path
import aiofiles


async def read_file(filename: Path) -> str:
    async with aiofiles.open(filename, encoding="utf-8", newline="\n") as file:
        return await file.read()


async def write_file(filename: Path, query: str):
    async with aiofiles.open(filename, "w", encoding="utf-8", newline="\n") as file:
        await file.write(query)


def get_sql_files(path: Path, recursive: bool) -> list[Path]:
    if path.is_dir():
        it = path.rglob("*.sql") if recursive else path.glob("*.sql")
        files = sorted(p for p in it if p.is_file())
        if not files:
            return []
    else:
        files = [path]
    return files

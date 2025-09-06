from pathlib import Path
import aiofiles


async def read_file(filename: Path) -> str:
    async with aiofiles.open(filename) as file:
        return await file.read()

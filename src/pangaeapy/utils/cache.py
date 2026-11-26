from pathlib import Path
from time import time

def read_cache(
    enable_cache: bool,
    cachedir: Path | None,
    cache_expiry_days: int | None,
    filename: str,
) -> str | None:
    if not enable_cache:
        return
    path = Path(cachedir, filename)
    content = None
    try:
        if time() - path.stat().st_mtime > 86400 * cache_expiry_days:
            path.unlink()
        else:
            content = path.read_text(encoding="utf-8")
    except OSError:
        pass
    return content

def write_cache(
    enable_cache: bool,
    cachedir: Path | None,
    filename: str,
    content: str,
) -> None:
    if not enable_cache:
        return
    path = Path(cachedir, filename)
    try:
        path.write_text(content, encoding="utf-8")
    except OSError:
        pass
    return

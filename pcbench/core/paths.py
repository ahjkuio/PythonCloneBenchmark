from pathlib import Path


def project_root_from(path: Path) -> Path:
    """Возвращает корень проекта (папка с pcbench), исходя из файла/каталога path."""
    p = path.resolve()
    # ожидаем структура: <root>/PythonCloneBenchmark_new/pcbench/...
    for parent in [p] + list(p.parents):
        if (parent / "pcbench").exists():
            return parent
    # fallback: родитель текущего
    return p.parent


def to_project_relative(path: Path, root: Path) -> str:
    """Строковый путь относительно корня проекта (POSIX стиль)."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return str(path.as_posix())

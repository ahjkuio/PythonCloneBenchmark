from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Optional


REQUIRED_COLS = {"file", "flines", "task", "username", "year"}


def read_gcj_csv(csv_path: Path, year: Optional[str] = None, limit_tasks: Optional[set[str]] = None) -> Iterable[dict]:
    """Итерирует строки GCJ CSV/TSV, нормализуя ключевые поля. Фильтрует по year/limit_tasks.

    Поддерживает запятые и табы как разделители. Возвращает словари вида:
    {year, task, username, file, flines}
    """
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        # Определяем диалект (запятая/таб) по образцу
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        except csv.Error:
            dialect = csv.excel  # по умолчанию запятая

        reader = csv.DictReader(f, dialect=dialect)
        fieldnames = reader.fieldnames or []
        missing = REQUIRED_COLS - set(fieldnames)
        if missing:
            raise ValueError(f"GCJ CSV missing columns: {missing}. Present: {fieldnames}")

        for row in reader:
            y = (row.get("year") or "").strip()
            task = (row.get("task") or "").strip()
            user = (row.get("username") or "").strip()
            fname = (row.get("file") or "").strip()
            code = row.get("flines")

            if not (y and task and user and fname and code is not None):
                continue
            if year is not None and y != str(year):
                continue
            if limit_tasks and task not in limit_tasks:
                continue
            yield {"year": y, "task": task, "username": user, "file": fname, "flines": code}

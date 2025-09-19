from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Optional


def _index_extracted_basenames(extracted_dir: Path, year: str) -> Dict[str, str]:
    """Строит отображение basename -> проектно-относительный путь под extracted_dir/year.
    Если встречаются дубликаты имен, запоминается первое попадание (для простоты).
    """
    # Нормализуем корни, чтобы избежать смешения относительных/абсолютных путей
    extracted_dir = extracted_dir.resolve()
    base = (extracted_dir / year).resolve()
    mapping: Dict[str, str] = {}
    for p in base.rglob("*.py"):
        name = p.name
        # Если структура стандартная: <root>/extracted_solutions/<year>/... — хотим путь от <root>
        # Иначе — от extracted_dir
        root_base = extracted_dir.parent if extracted_dir.name == "extracted_solutions" else extracted_dir
        root_base = root_base.resolve()
        rel = p.relative_to(root_base)
        rel_str = rel.as_posix()
        if name not in mapping:
            mapping[name] = rel_str
    return mapping


def convert_ccaligner_to_tool_csv(
    ccaligner_csv: Path,
    extracted_dir: Path,
    year: str,
    output_csv: Path,
    assume_1_indexed_input: bool = False,
) -> Path:
    """Конвертирует вывод CCAligner (dir1,name1,start1,end1,dir2,name2,start2,end2)
    в формат tool CSV (file1_path,file1_start,file1_end,file2_path,file2_start,file2_end).
    Пытается сопоставить по basename с извлеченными решениями.
    """
    mapping = _index_extracted_basenames(extracted_dir, year)

    with ccaligner_csv.open("r", encoding="utf-8", newline="") as fin, \
         output_csv.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.reader(fin)
        fieldnames = [
            "file1_path",
            "file1_start",
            "file1_end",
            "file2_path",
            "file2_start",
            "file2_end",
        ]
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        converted = 0
        skipped = 0
        for row in reader:
            if len(row) != 8:
                skipped += 1
                continue
            _, name1, s1, e1, _, name2, s2, e2 = row
            p1 = mapping.get(name1)
            p2 = mapping.get(name2)
            if not p1 or not p2:
                skipped += 1
                continue
            try:
                s1i = int(s1); e1i = int(e1); s2i = int(s2); e2i = int(e2)
                if assume_1_indexed_input:
                    s1i -= 1; e1i -= 1; s2i -= 1; e2i -= 1
                writer.writerow(
                    {
                        "file1_path": p1,
                        "file1_start": s1i,
                        "file1_end": e1i,
                        "file2_path": p2,
                        "file2_start": s2i,
                        "file2_end": e2i,
                    }
                )
                converted += 1
            except Exception:
                skipped += 1
                continue
    # Можно вернуть путь и вывести статистику в вызывающем коде
    return output_csv

from __future__ import annotations

import csv
from pathlib import Path
import gzip
from typing import Iterable


def generate_mock_from_benchmark(
    benchmark_csv: Path,
    output_csv: Path,
    take_first_n: int = 100,
    add_false_m: int = 20,
) -> Path:
    """Создает псевдо-вывод детектора: берёт N первых пар из бенчмарка и добавляет M ложных.
    Полезно для регрессионной проверки пайплайна.
    """
    true_rows = []
    # Поддержим .csv.gz
    if str(benchmark_csv).endswith(".gz"):
        fh = gzip.open(benchmark_csv, "rt", encoding="utf-8", newline="")
    else:
        fh = benchmark_csv.open("r", encoding="utf-8", newline="")
    with fh as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            if i >= take_first_n:
                break
            true_rows.append(
                {
                    "file1_path": row["file1_path"],
                    "file1_start": row["file1_start"],
                    "file1_end": row["file1_end"],
                    "file2_path": row["file2_path"],
                    "file2_start": row["file2_start"],
                    "file2_end": row["file2_end"],
                }
            )

    fp_rows = [
        {
            "file1_path": f"nonexistent/{i}_a.py",
            "file1_start": 0,
            "file1_end": 3,
            "file2_path": f"nonexistent/{i}_b.py",
            "file2_start": 0,
            "file2_end": 3,
        }
        for i in range(add_false_m)
    ]

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "file1_path",
                "file1_start",
                "file1_end",
                "file2_path",
                "file2_start",
                "file2_end",
            ],
        )
        w.writeheader()
        for r in true_rows:
            w.writerow(r)
        for r in fp_rows:
            w.writerow(r)
    return output_csv

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Tuple

from ..core.paths import to_project_relative


def _parse_line(line: str) -> Tuple[str, int, int, str, int, int] | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    for delimiter in ("\t", ","):
        reader = csv.reader([line], delimiter=delimiter)
        row = next(reader)
        if len(row) >= 6:
            parts = row
            break
    else:
        parts = line.split()

    if len(parts) < 6:
        return None

    try:
        start1 = int(float(parts[1]))
        end1 = int(float(parts[2]))
        start2 = int(float(parts[4]))
        end2 = int(float(parts[5]))
    except ValueError:
        return None

    return parts[0], start1, end1, parts[3], start2, end2


def _iter_pairs(pairs_file: Path) -> Iterable[Tuple[str, int, int, str, int, int]]:
    for line in pairs_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        parsed = _parse_line(line)
        if parsed is not None:
            yield parsed


def convert_sourcerercc(
    pairs_file: Path,
    project_root: Path,
    output_csv: Path,
) -> Path:
    project_root = project_root.resolve()
    rows = []
    for path1, start1, end1, path2, start2, end2 in _iter_pairs(pairs_file):
        rows.append(
            {
                "file1_path": to_project_relative(Path(path1), project_root),
                "file1_start": start1,
                "file1_end": end1,
                "file2_path": to_project_relative(Path(path2), project_root),
                "file2_start": start2,
                "file2_end": end2,
            }
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "file1_path",
                "file1_start",
                "file1_end",
                "file2_path",
                "file2_start",
                "file2_end",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_csv


__all__ = ["convert_sourcerercc"]

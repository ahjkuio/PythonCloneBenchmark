from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional


def _to_relative(path: Path, extracted_dir: Path) -> str:
    """Return POSIX-style path relative to project root/extracted dir when possible."""
    extracted_dir = extracted_dir.resolve()
    project_root = extracted_dir.parent
    candidate = path.expanduser().resolve(strict=False)

    for base in (project_root, extracted_dir):
        try:
            return candidate.relative_to(base).as_posix()
        except ValueError:
            continue
    return candidate.as_posix()


def _convert_line_values(values: list[str], assume_1_indexed: bool) -> Optional[dict]:
    if len(values) != 6:
        return None
    try:
        s1 = int(float(values[1]))
        e1 = int(float(values[2]))
        s2 = int(float(values[4]))
        e2 = int(float(values[5]))
    except ValueError:
        return None
    if assume_1_indexed:
        s1 -= 1
        e1 -= 1
        s2 -= 1
        e2 -= 1
    return {
        "file1_path": values[0],
        "file1_start": max(s1, 0),
        "file1_end": max(e1, 0),
        "file2_path": values[3],
        "file2_start": max(s2, 0),
        "file2_end": max(e2, 0),
    }


def convert_nil_to_tool_csv(
    nil_csv: Path,
    extracted_dir: Path,
    output_csv: Path,
    assume_1_indexed_input: bool = True,
) -> Path:
    """Convert NIL CSV (file,start,end,file,start,end) to pcbench tool CSV format."""
    rows: list[dict] = []
    with nil_csv.open("r", encoding="utf-8", newline="") as fin:
        reader = csv.reader(fin)
        for raw in reader:
            converted = _convert_line_values(raw, assume_1_indexed_input)
            if not converted:
                continue
            rows.append(converted)

    if not rows:
        output_csv.write_text("file1_path,file1_start,file1_end,file2_path,file2_start,file2_end\n", encoding="utf-8")
        return output_csv

    with output_csv.open("w", encoding="utf-8", newline="") as fout:
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
        for entry in rows:
            entry = entry.copy()
            entry["file1_path"] = _to_relative(Path(entry["file1_path"]), extracted_dir)
            entry["file2_path"] = _to_relative(Path(entry["file2_path"]), extracted_dir)
            writer.writerow(entry)
    return output_csv

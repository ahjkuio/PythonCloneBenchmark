from __future__ import annotations

import csv
import itertools
import xml.etree.ElementTree as ET
from pathlib import Path

from ..core.paths import to_project_relative


def convert_nicad_clusters(
    xml_path: Path,
    project_root: Path,
    output_csv: Path,
) -> Path:
    project_root = project_root.resolve()
    resolved_xml = xml_path.resolve()
    tree = ET.parse(resolved_xml)
    root = tree.getroot()

    nicad_root = resolved_xml.parent
    while nicad_root.name != "nicadclones" and nicad_root != nicad_root.parent:
        nicad_root = nicad_root.parent
    nicad_root = nicad_root.parent if nicad_root.name == "nicadclones" else resolved_xml.parent

    rows = []
    clone_nodes = root.findall(".//clone")
    if not clone_nodes:
        clone_nodes = root.findall(".//class")

    for clone in clone_nodes:
        sources = clone.findall("source")
        for left, right in itertools.combinations(sources, 2):
            try:
                row = {
                    "file1_path": to_project_relative((nicad_root / Path(left.attrib["file"])).resolve(), project_root),
                    "file1_start": int(left.attrib.get("startline", "0")),
                    "file1_end": int(left.attrib.get("endline", "0")),
                    "file2_path": to_project_relative((nicad_root / Path(right.attrib["file"])).resolve(), project_root),
                    "file2_start": int(right.attrib.get("startline", "0")),
                    "file2_end": int(right.attrib.get("endline", "0")),
                }
            except (KeyError, ValueError):
                continue
            rows.append(row)

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


__all__ = ["convert_nicad_clusters"]

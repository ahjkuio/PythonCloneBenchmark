from pathlib import Path

import xml.etree.ElementTree as ET

from pcbench.adapters.sourcerercc import convert_sourcerercc
from pcbench.adapters.nicad import convert_nicad_clusters


def test_convert_sourcerercc(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "file_a.py").write_text("print('a')\n", encoding="utf-8")
    (project_root / "file_b.py").write_text("print('b')\n", encoding="utf-8")

    pairs_txt = tmp_path / "pairs.txt"
    pairs_txt.write_text(
        f"{project_root / 'file_a.py'}\t1\t5\t{project_root / 'file_b.py'}\t2\t6\n",
        encoding="utf-8",
    )

    output_csv = tmp_path / "tool.csv"
    convert_sourcerercc(pairs_txt, project_root, output_csv)

    data = output_csv.read_text(encoding="utf-8").splitlines()
    assert data[0] == "file1_path,file1_start,file1_end,file2_path,file2_start,file2_end"
    assert data[1] == "file_a.py,1,5,file_b.py,2,6"


def test_convert_nicad(tmp_path: Path) -> None:
    project_root = tmp_path / "workspace"
    project_root.mkdir()
    (project_root / "a.py").write_text("print('a')\n", encoding="utf-8")
    (project_root / "b.py").write_text("print('b')\n", encoding="utf-8")

    clones = ET.Element("clones")
    clone = ET.SubElement(clones, "clone")
    ET.SubElement(
        clone,
        "source",
        file=str(project_root / "a.py"),
        startline="3",
        endline="9",
    )
    ET.SubElement(
        clone,
        "source",
        file=str(project_root / "b.py"),
        startline="4",
        endline="8",
    )

    xml_path = tmp_path / "clusters.xml"
    ET.ElementTree(clones).write(xml_path, encoding="utf-8")

    output_csv = tmp_path / "tool.csv"
    convert_nicad_clusters(xml_path, project_root, output_csv)

    lines = output_csv.read_text(encoding="utf-8").splitlines()
    assert lines[1] == "a.py,3,9,b.py,4,8"

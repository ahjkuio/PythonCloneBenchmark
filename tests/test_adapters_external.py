from pathlib import Path

import xml.etree.ElementTree as ET

from pcbench.adapters.sourcerercc import convert_sourcerercc
from pcbench.adapters.nicad import convert_nicad_clusters
from pcbench.adapters.pythonclonedetection import convert_pythonclonedetection


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


def test_convert_pythonclonedetection(tmp_path: Path) -> None:
    raw_csv = tmp_path / "raw.csv"
    raw_csv.write_text(
        """file1_path,file1_start,file1_end,file2_path,file2_start,file2_end,predictions,score
a.py,1,10,b.py,5,15,1,0.9
a.py,1,10,c.py,3,8,1,0.3
a.py,1,10,d.py,2,6,0,0.8
""",
        encoding="utf-8",
    )

    output_csv = tmp_path / "tool.csv"
    convert_pythonclonedetection(raw_csv, output_csv, min_score=0.5)

    rows = output_csv.read_text(encoding="utf-8").splitlines()
    assert rows == [
        "file1_path,file1_start,file1_end,file2_path,file2_start,file2_end",
        "a.py,1,10,b.py,5,15",
    ]


def test_convert_sourcerercc_with_stats(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "project" / "a.py").parent.mkdir(parents=True, exist_ok=True)
    (project_root / "project" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (project_root / "project" / "b.py").write_text("print('b')\n", encoding="utf-8")

    stats = tmp_path / "files-stats-0.stats"
    stats.write_text(
        """1,1,"/abs/a.py","project/a.py","hash",10,5,0,0
1,2,"/abs/b.py","project/b.py","hash",12,3,0,0
""",
        encoding="utf-8",
    )

    pairs = tmp_path / "results.pairs"
    pairs.write_text("1,1,1,2\n", encoding="utf-8")

    output_csv = tmp_path / "tool.csv"
    convert_sourcerercc(pairs, project_root, output_csv, stats_paths=[stats])

    lines = output_csv.read_text(encoding="utf-8").splitlines()
    assert lines[1] == "project/a.py,0,4,project/b.py,0,2"

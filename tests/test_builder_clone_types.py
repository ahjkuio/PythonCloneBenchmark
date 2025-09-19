import csv
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.core.builder import build_benchmark


def _write_gcj_csv(path: Path, rows):
    fieldnames = ["year", "task", "username", "file", "flines"]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_build_benchmark_with_clone_type_annotation():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        csv_path = tmp_path / "gcj.csv"
        extracted = tmp_path / "extracted"
        output = tmp_path / "out"
        code = """
def solve():
    return 1
""".lstrip()
        rows = [
            {"year": "2017", "task": "A", "username": "u1", "file": "main.py", "flines": code},
            {"year": "2017", "task": "A", "username": "u2", "file": "main.py", "flines": code},
        ]
        _write_gcj_csv(csv_path, rows)

        out_path = build_benchmark(
            year="2017",
            input_csv_path=csv_path,
            extracted_dir=extracted,
            output_dir=output,
            granularity="file",
            min_lines=1,
            annotate_clone_type=True,
        )

        # Single CSV expected
        csv_file = output / "clones_2017.csv"
        assert csv_file.exists()
        with csv_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert row["clone_type"] == "type1"

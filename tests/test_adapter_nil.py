import csv
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.adapters.nil import convert_nil_to_tool_csv


def read_csv(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def test_convert_nil_to_tool_csv_relative_paths():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        project = tmp_path / "proj"
        extracted_dir = project / "extracted_solutions"
        sample_dir = extracted_dir / "2017" / "TASK" / "user"
        sample_dir.mkdir(parents=True, exist_ok=True)
        file1 = sample_dir / "a.py"
        file2 = sample_dir / "b.py"
        file1.write_text("print('a')\n", encoding="utf-8")
        file2.write_text("print('b')\n", encoding="utf-8")

        nil_csv = tmp_path / "nil.csv"
        nil_csv.write_text(
            f"{file1.resolve()},{1},{10},{file2.resolve()},{5},{15}\n",
            encoding="utf-8",
        )
        output = tmp_path / "tool.csv"

        convert_nil_to_tool_csv(nil_csv, extracted_dir, output)

        rows = read_csv(output)
        assert len(rows) == 1
        row = rows[0]
        assert row["file1_path"] == "extracted_solutions/2017/TASK/user/a.py"
        assert row["file2_path"] == "extracted_solutions/2017/TASK/user/b.py"
        assert row["file1_start"] == "0"
        assert row["file1_end"] == "9"
        assert row["file2_start"] == "4"
        assert row["file2_end"] == "14"

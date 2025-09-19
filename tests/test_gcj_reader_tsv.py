import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.core.gcj_reader import read_gcj_csv


def test_read_gcj_csv_tsv():
    # Таб-разделенный заголовок без ведущей запятой
    header = "year\tround\tusername\ttask\tsolution\tfile\tfull_path\tflines\n"
    row1 = "2017\tqual\tuser1\ttaskA\tS\tsol.py\t/path\tprint('ok')\n"
    row2 = "2017\tqual\tuser2\ttaskB\tS\tsol2.py\t/path\tprint('ok2')\n"
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "gcj.tsv"
        p.write_text(header + row1 + row2, encoding="utf-8")
        rows = list(read_gcj_csv(p, year="2017", limit_tasks={"taskA"}))
        assert len(rows) == 1
        r = rows[0]
        assert r["file"] == "sol.py"
        assert r["username"] == "user1"
        assert r["task"] == "taskA"

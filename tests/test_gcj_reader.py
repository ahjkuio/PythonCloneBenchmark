import tempfile
from pathlib import Path
import sys

# Ensure project root on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.core.gcj_reader import read_gcj_csv


def write_csv(tmp: Path, text: str) -> Path:
    p = tmp / "gcj.csv"
    p.write_text(text, encoding="utf-8")
    return p


def test_read_gcj_csv_filters_and_fields():
    # Minimal CSV with required columns
    csv_text = ",file,flines,full_path,round,solution,task,username,year\n" \
               "0,sol.py,print('ok'),/path,qual,A,task123,user1,2017\n" \
               "1,sol.py,print('no'),/path,qual,A,task999,user2,2016\n"
    with tempfile.TemporaryDirectory() as d:
        p = write_csv(Path(d), csv_text)
        rows = list(read_gcj_csv(p, year="2017", limit_tasks={"task123"}))
        assert len(rows) == 1
        r = rows[0]
        assert r["file"] == "sol.py"
        assert r["flines"].startswith("print")
        assert r["task"] == "task123"
        assert r["username"] == "user1"
        assert r["year"] == "2017"

import tempfile
from pathlib import Path
import sys
import csv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.core.evaluator import evaluate


def write_csv(path: Path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_evaluator_simple():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        bench = d / "bench.csv"
        tool = d / "tool.csv"

        bench_rows = [
            {
                "file1_path": "a.py",
                "file1_start": 0,
                "file1_end": 9,
                "file2_path": "b.py",
                "file2_start": 0,
                "file2_end": 9,
                "granularity1": "file",
                "granularity2": "file",
                "task_id": "T1",
            },
            {
                "file1_path": "c.py",
                "file1_start": 0,
                "file1_end": 9,
                "file2_path": "d.py",
                "file2_start": 0,
                "file2_end": 9,
                "granularity1": "file",
                "granularity2": "file",
                "task_id": "T2",
            },
        ]
        tool_rows = [
            {  # exact match for first pair
                "file1_path": "a.py",
                "file1_start": 0,
                "file1_end": 9,
                "file2_path": "b.py",
                "file2_start": 0,
                "file2_end": 9,
            },
            {  # false positive
                "file1_path": "x.py",
                "file1_start": 0,
                "file1_end": 3,
                "file2_path": "y.py",
                "file2_start": 0,
                "file2_end": 3,
            },
        ]

        write_csv(
            bench,
            [
                "file1_path",
                "file1_start",
                "file1_end",
                "file2_path",
                "file2_start",
                "file2_end",
                "granularity1",
                "granularity2",
                "task_id",
            ],
            bench_rows,
        )
        write_csv(
            tool,
            [
                "file1_path",
                "file1_start",
                "file1_end",
                "file2_path",
                "file2_start",
                "file2_end",
            ],
            tool_rows,
        )

        res = evaluate(bench, tool, metric="c", threshold=0.7)
        assert res["TP"] == 1
        assert res["FP"] == 1
        assert res["FN"] == 1
        assert abs(res["precision"] - 0.5) < 1e-9
        assert abs(res["recall"] - 0.5) < 1e-9

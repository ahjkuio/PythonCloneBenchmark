from __future__ import annotations

import csv
from pathlib import Path
import gzip
from typing import Dict, Iterable, List, Tuple, Set, Optional

from .eval_metrics import calculate_fragment_coverage


def _open_text(path: Path):
    return gzip.open(path, "rt", encoding="utf-8", newline="") if str(path).endswith(".gz") else path.open("r", encoding="utf-8", newline="")


def _read_benchmark_csv(path: Path) -> Iterable[dict]:
    with _open_text(path) as f:
        reader = csv.DictReader(f)
        required = {
            "file1_path",
            "file1_start",
            "file1_end",
            "file2_path",
            "file2_start",
            "file2_end",
            "task_id",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Benchmark CSV missing columns: {missing}")
        for row in reader:
            try:
                yield {
                    "file1_path": row["file1_path"],
                    "file1_start": int(row["file1_start"]),
                    "file1_end": int(row["file1_end"]),
                    "file2_path": row["file2_path"],
                    "file2_start": int(row["file2_start"]),
                    "file2_end": int(row["file2_end"]),
                    "task_id": row.get("task_id"),
                }
            except Exception:
                continue


def _read_tool_csv(path: Path) -> Iterable[dict]:
    with _open_text(path) as f:
        reader = csv.DictReader(f)
        required = {
            "file1_path",
            "file1_start",
            "file1_end",
            "file2_path",
            "file2_start",
            "file2_end",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Tool CSV missing columns: {missing}")
        for row in reader:
            try:
                yield {
                    "file1_path": row["file1_path"],
                    "file1_start": int(float(row["file1_start"])) if row["file1_start"] else 0,
                    "file1_end": int(float(row["file1_end"])) if row["file1_end"] else 0,
                    "file2_path": row["file2_path"],
                    "file2_start": int(float(row["file2_start"])) if row["file2_start"] else 0,
                    "file2_end": int(float(row["file2_end"])) if row["file2_end"] else 0,
                }
            except Exception:
                continue


def _pair_key(p1: str, p2: str) -> Tuple[str, str]:
    return (p1, p2) if p1 <= p2 else (p2, p1)


def _c_match(b: dict, t: dict, threshold: float) -> bool:
    # coverage of benchmark by tool must exceed threshold for both fragments
    # match either (f1==f1 & f2==f2) or swapped
    if b["file1_path"] == t["file1_path"] and b["file2_path"] == t["file2_path"]:
        coords = ((b["file1_start"], b["file1_end"], t["file1_start"], t["file1_end"]),
                  (b["file2_start"], b["file2_end"], t["file2_start"], t["file2_end"]))
    elif b["file1_path"] == t["file2_path"] and b["file2_path"] == t["file1_path"]:
        coords = ((b["file1_start"], b["file1_end"], t["file2_start"], t["file2_end"]),
                  (b["file2_start"], b["file2_end"], t["file1_start"], t["file1_end"]))
    else:
        return False
    for bs, be, ts, te in coords:
        cov_b_by_t, _ = calculate_fragment_coverage(bs, be, ts, te)
        if cov_b_by_t < threshold:
            return False
    return True


def _sc_match(b: dict, t: dict, threshold: float) -> bool:
    if b["file1_path"] == t["file1_path"] and b["file2_path"] == t["file2_path"]:
        coords = ((b["file1_start"], b["file1_end"], t["file1_start"], t["file1_end"]),
                  (b["file2_start"], b["file2_end"], t["file2_start"], t["file2_end"]))
    elif b["file1_path"] == t["file2_path"] and b["file2_path"] == t["file1_path"]:
        coords = ((b["file1_start"], b["file1_end"], t["file2_start"], t["file2_end"]),
                  (b["file2_start"], b["file2_end"], t["file1_start"], t["file1_end"]))
    else:
        return False
    for bs, be, ts, te in coords:
        cov_b_by_t, cov_t_by_b = calculate_fragment_coverage(bs, be, ts, te)
        if cov_b_by_t < threshold or cov_t_by_b < threshold:
            return False
    return True


def _fc_match(b: dict, t: dict, threshold: float, epsilon: float) -> bool:
    # c-match must pass; and tool must overlap benchmark by at least epsilon in both frags
    if not _c_match(b, t, threshold):
        return False
    if b["file1_path"] == t["file1_path"] and b["file2_path"] == t["file2_path"]:
        coords = ((b["file1_start"], b["file1_end"], t["file1_start"], t["file1_end"]),
                  (b["file2_start"], b["file2_end"], t["file2_start"], t["file2_end"]))
    else:
        coords = ((b["file1_start"], b["file1_end"], t["file2_start"], t["file2_end"]),
                  (b["file2_start"], b["file2_end"], t["file1_start"], t["file1_end"]))
    for bs, be, ts, te in coords:
        _, cov_t_by_b = calculate_fragment_coverage(bs, be, ts, te)
        if cov_t_by_b < epsilon:
            return False
    return True


def evaluate(benchmark_csv: Path, tool_csv: Path, metric: str = "c", threshold: float = 0.7, epsilon: float = 1e-10) -> Dict[str, float]:
    # Read tool rows and index by unordered pair of paths
    tool_rows: List[dict] = list(_read_tool_csv(tool_csv))
    index: Dict[Tuple[str, str], List[int]] = {}
    for i, t in enumerate(tool_rows):
        key = _pair_key(t["file1_path"], t["file2_path"])
        index.setdefault(key, []).append(i)

    matched_tool: Set[int] = set()
    TP = 0

    if metric == "c":
        matcher = lambda b, t: _c_match(b, t, threshold)
    elif metric == "sc":
        matcher = lambda b, t: _sc_match(b, t, threshold)
    else:  # fc
        matcher = lambda b, t: _fc_match(b, t, threshold, epsilon)

    for b in _read_benchmark_csv(benchmark_csv):
        key = _pair_key(b["file1_path"], b["file2_path"])
        hit = False
        for ti in index.get(key, []):
            t = tool_rows[ti]
            if matcher(b, t):
                matched_tool.add(ti)
                hit = True
                break
        if hit:
            TP += 1

    total_tool = len(tool_rows)
    FP = total_tool - len(matched_tool)
    # FN: бенчмарк-пары, которые не были покрыты инструментом
    total_benchmark = sum(1 for _ in _read_benchmark_csv(benchmark_csv))
    FN = total_benchmark - TP

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0.0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "TP": TP,
        "FP": FP,
        "FN": FN,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "total_benchmark": total_benchmark,
        "total_tool": total_tool,
    }


def evaluate_with_details(
    benchmark_csv: Path,
    tool_csv: Path,
    metric: str = "c",
    threshold: float = 0.7,
    epsilon: float = 1e-10,
    sample_k: int = 50,
) -> Tuple[Dict[str, float], List[dict], List[dict], List[dict]]:
    """Возвращает метрики и сэмплы TP/FP/FN (по sample_k элементов максимум)."""
    tool_rows: List[dict] = list(_read_tool_csv(tool_csv))
    index: Dict[Tuple[str, str], List[int]] = {}
    for i, t in enumerate(tool_rows):
        key = _pair_key(t["file1_path"], t["file2_path"])
        index.setdefault(key, []).append(i)

    matched_tool: Set[int] = set()
    TP = 0
    tp_samples: List[dict] = []
    fn_samples: List[dict] = []

    if metric == "c":
        matcher = lambda b, t: _c_match(b, t, threshold)
    elif metric == "sc":
        matcher = lambda b, t: _sc_match(b, t, threshold)
    else:
        matcher = lambda b, t: _fc_match(b, t, threshold, epsilon)

    for b in _read_benchmark_csv(benchmark_csv):
        key = _pair_key(b["file1_path"], b["file2_path"])
        hit = False
        for ti in index.get(key, []):
            t = tool_rows[ti]
            if matcher(b, t):
                matched_tool.add(ti)
                hit = True
                if len(tp_samples) < sample_k:
                    tp_samples.append({"benchmark": b, "tool": t})
                break
        if hit:
            TP += 1
        else:
            if len(fn_samples) < sample_k:
                fn_samples.append(b)

    total_tool = len(tool_rows)
    FP = total_tool - len(matched_tool)
    total_benchmark = sum(1 for _ in _read_benchmark_csv(benchmark_csv))
    FN = total_benchmark - TP
    precision = TP / (TP + FP) if (TP + FP) > 0 else 0.0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    # Collect FP samples
    fp_samples: List[dict] = []
    if FP > 0:
        for i, t in enumerate(tool_rows):
            if i not in matched_tool:
                fp_samples.append(t)
                if len(fp_samples) >= sample_k:
                    break

    metrics = {
        "TP": TP,
        "FP": FP,
        "FN": FN,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "total_benchmark": total_benchmark,
        "total_tool": total_tool,
    }
    return metrics, tp_samples, fp_samples, fn_samples


def write_eval_report(
    out_dir: Path,
    metrics: Dict[str, float],
    tp: List[dict],
    fp: List[dict],
    fn: List[dict],
    metric_name: str,
    threshold: float,
    epsilon: float,
):
    out_dir.mkdir(parents=True, exist_ok=True)
    # Markdown summary
    md = out_dir / "summary.md"
    with md.open("w", encoding="utf-8") as f:
        f.write("# Evaluation Summary\n\n")
        f.write(f"- Metric: {metric_name}\n")
        f.write(f"- Threshold: {threshold}\n")
        if metric_name == "fc":
            f.write(f"- Epsilon: {epsilon}\n")
        f.write(f"- TP: {metrics['TP']}  FP: {metrics['FP']}  FN: {metrics['FN']}\n")
        f.write(f"- Precision: {metrics['precision']:.4f}\n")
        f.write(f"- Recall: {metrics['recall']:.4f}\n")
        f.write(f"- F1: {metrics['f1']:.4f}\n")
        f.write(f"- Totals: benchmark={metrics['total_benchmark']} tool={metrics['total_tool']}\n")

    # Write samples CSVs
    def write_csv(path: Path, rows: List[dict], fieldnames: List[str]):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)

    # Flatten TP pair into one row for readability
    tp_rows = []
    for pair in tp:
        br = pair["benchmark"]
        tr = pair["tool"]
        merged = {f"b_{k}": v for k, v in br.items()}
        merged.update({f"t_{k}": v for k, v in tr.items()})
        tp_rows.append(merged)

    if tp_rows:
        write_csv(out_dir / "tp.csv", tp_rows, fieldnames=list(tp_rows[0].keys()))
    if fp:
        write_csv(out_dir / "fp.csv", fp, fieldnames=list(fp[0].keys()))
    if fn:
        write_csv(out_dir / "fn.csv", fn, fieldnames=list(fn[0].keys()))

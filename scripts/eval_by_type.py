#!/usr/bin/env python3
"""Рассчитать метрики по типам клонов для результатов детектора.

Пример:
  python scripts/eval_by_type.py \
      --benchmark benchmark_output/clones_2017.csv.gz \
      --tool data/tool_nil_small.csv \
      --metric c --threshold 0.7

Выводит таблицу (Markdown) с total/TP/FN/recall по каждому clone_type.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import pandas as pd

from pcbench.core import evaluator as ev


KEY = [
    "file1_path",
    "file1_start",
    "file1_end",
    "file2_path",
    "file2_start",
    "file2_end",
]


def load_benchmark(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "clone_type" not in df.columns:
        raise ValueError("Benchmark CSV must contain clone_type column")
    return df[KEY + ["clone_type"]]


def build_tool_index(tool_rows):
    index: Dict[tuple[str, str], list[int]] = {}
    for i, t in enumerate(tool_rows):
        key = ev._pair_key(t["file1_path"], t["file2_path"])
        index.setdefault(key, []).append(i)
    return index


def select_matcher(metric: str, threshold: float, epsilon: float):
    if metric == "c":
        return lambda b, t: ev._c_match(b, t, threshold)
    if metric == "sc":
        return lambda b, t: ev._sc_match(b, t, threshold)
    if metric == "fc":
        return lambda b, t: ev._fc_match(b, t, threshold, epsilon)
    raise ValueError(f"Unsupported metric: {metric}")


def evaluate_by_type(bench: pd.DataFrame, tool_csv: Path, metric: str, threshold: float, epsilon: float):
    tool_rows = list(ev._read_tool_csv(tool_csv))
    index = build_tool_index(tool_rows)
    matcher = select_matcher(metric, threshold, epsilon)

    matched_tool = set()
    tp_by_type: Dict[str, int] = {}

    # iterate benchmark
    for row in bench.to_dict("records"):
        key = ev._pair_key(row["file1_path"], row["file2_path"])
        hit = False
        for idx in index.get(key, []):
            tool_row = tool_rows[idx]
            if matcher(row, tool_row):
                matched_tool.add(idx)
                hit = True
                break
        if hit:
            tp_by_type[row["clone_type"]] = tp_by_type.get(row["clone_type"], 0) + 1

    total_by_type = bench.groupby("clone_type").size().to_dict()

    stats = []
    for clone_type, total in sorted(total_by_type.items()):
        tp = tp_by_type.get(clone_type, 0)
        fn = total - tp
        recall = tp / total if total else 0.0
        stats.append({
            "clone_type": clone_type,
            "total": total,
            "TP": tp,
            "FN": fn,
            "recall": recall,
        })

    fp = len(tool_rows) - len(matched_tool)

    return stats, len(tool_rows), fp


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate detector results per clone type")
    parser.add_argument("--benchmark", required=True, type=Path)
    parser.add_argument("--tool", required=True, type=Path)
    parser.add_argument("--metric", choices=["c", "sc", "fc"], default="c")
    parser.add_argument("--threshold", type=float, default=0.7)
    parser.add_argument("--epsilon", type=float, default=1e-10)
    args = parser.parse_args()

    bench = load_benchmark(args.benchmark)
    stats, total_tool, fp = evaluate_by_type(bench, args.tool, args.metric, args.threshold, args.epsilon)

    print("| clone_type | total | TP | FN | recall |")
    print("|---|---:|---:|---:|---:|")
    for row in stats:
        print(
            f"| {row['clone_type']} | {row['total']} | {row['TP']} | {row['FN']} | {row['recall']:.4f} |"
        )
    print()
    print(f"Total tool pairs: {total_tool}, FP (unmatched tool pairs): {fp}")


if __name__ == "__main__":
    main()


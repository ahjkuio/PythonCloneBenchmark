#!/usr/bin/env python3

import argparse
import itertools
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Segment:
    file_path: Path
    start: int
    end: int
    task_id: str
    code: str


def read_snippet(project_root: Path, rel_path: str, start: int, end: int, task_id: str) -> Segment:
    rel = Path(rel_path)
    abs_path = (project_root / rel).resolve()
    with abs_path.open("r", encoding="utf-8") as src:
        lines = src.readlines()
    snippet = "".join(lines[start : end + 1])
    return Segment(rel, start, end, task_id, snippet)


def collect_segments(benchmark_csv: Path, project_root: Path) -> list[Segment]:
    df = pd.read_csv(benchmark_csv)
    segments_map: dict[tuple[str, int, int], Segment] = {}
    for row in df.itertuples(index=False):
        entries = (
            (row.file1_path, int(row.file1_start), int(row.file1_end)),
            (row.file2_path, int(row.file2_start), int(row.file2_end)),
        )
        for rel_path, start, end in entries:
            key = (rel_path, start, end)
            if key not in segments_map:
                segments_map[key] = read_snippet(project_root, rel_path, start, end, str(row.task_id))
    return list(segments_map.values())


def build_pairs(segments: list[Segment], within_task_only: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    codes = []
    meta_rows = []
    if within_task_only:
        by_task: dict[str, list[Segment]] = {}
        for segment in segments:
            by_task.setdefault(segment.task_id, []).append(segment)
        iterables = (itertools.combinations(bucket, 2) for bucket in by_task.values())
        pair_iter = itertools.chain.from_iterable(iterables)
    else:
        pair_iter = itertools.combinations(segments, 2)

    for left, right in pair_iter:
        codes.append({"code1": left.code, "code2": right.code})
        meta_rows.append(
            {
                "file1_path": left.file_path.as_posix(),
                "file1_start": left.start,
                "file1_end": left.end,
                "file2_path": right.file_path.as_posix(),
                "file2_start": right.start,
                "file2_end": right.end,
                "task_id_left": left.task_id,
                "task_id_right": right.task_id,
            }
        )
    codes_df = pd.DataFrame(codes)
    meta_df = pd.DataFrame(meta_rows)
    return codes_df, meta_df


def run_detector(
    pairs_df: pd.DataFrame,
    python_clonedetection_root: Path,
    max_token_size: int,
    batch_size: int,
) -> pd.DataFrame:
    sys.path.insert(0, str(python_clonedetection_root))
    from clone_classifier import CloneClassifier

    classifier = CloneClassifier(
        max_token_size=max_token_size,
        fp16=False,
        per_device_eval_batch_size=batch_size,
    )
    predictions = classifier.predict(pairs_df[["code1", "code2"]], return_score=True)
    return predictions


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run PythonCloneDetection GraphCodeBERT on medium benchmark pairs."
    )
    parser.add_argument(
        "--benchmark-csv",
        default=REPO_ROOT / "benchmark_output_medium" / "clones_2017.csv",
        type=Path,
        help="Benchmark CSV with reference pairs.",
    )
    parser.add_argument(
        "--project-root",
        default=REPO_ROOT,
        type=Path,
        help="Root of the benchmark repository (used to resolve file paths).",
    )
    parser.add_argument(
        "--pythonclonedetection-root",
        default=REPO_ROOT.parent / "PythonCloneDetection",
        type=Path,
        help="Path to the PythonCloneDetection repository clone.",
    )
    parser.add_argument(
        "--raw-output",
        default=REPO_ROOT / "benchmark_output_medium" / "pythonclonedetection_raw.csv",
        type=Path,
        help="Where to save raw detector predictions.",
    )
    parser.add_argument(
        "--tool-csv",
        default=REPO_ROOT / "data" / "tool_pythonclonedetection_medium.csv",
        type=Path,
        help="Where to save tool-format CSV.",
    )
    parser.add_argument(
        "--max-token-size",
        default=512,
        type=int,
        help="Maximum token length for the detector model.",
    )
    parser.add_argument(
        "--batch-size",
        default=8,
        type=int,
        help="Batch size for inference (per device).",
    )
    parser.add_argument(
        "--all-pairs",
        action="store_true",
        help="Include cross-task pairs (significantly slower).",
    )
    args = parser.parse_args()

    project_root = args.project_root.resolve()
    segments = collect_segments(args.benchmark_csv, project_root)
    pairs_df, meta_df = build_pairs(segments, within_task_only=not args.all_pairs)

    predictions_df = run_detector(
        pairs_df,
        args.pythonclonedetection_root.resolve(),
        args.max_token_size,
        args.batch_size,
    )

    payload = [meta_df.reset_index(drop=True)]
    for column in ("predictions", "score"):
        if column in predictions_df.columns:
            payload.append(predictions_df[[column]])
    merged = pd.concat(payload, axis=1)
    merged.to_csv(args.raw_output, index=False)

    positives = merged[merged["predictions"] == 1].copy()
    positives[
        ["file1_path", "file1_start", "file1_end", "file2_path", "file2_start", "file2_end"]
    ].to_csv(args.tool_csv, index=False)


if __name__ == "__main__":
    main()

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional


REQUIRED_COLUMNS = {
    "file1_path",
    "file1_start",
    "file1_end",
    "file2_path",
    "file2_start",
    "file2_end",
}


def convert_pythonclonedetection(
    raw_csv: Path,
    output_csv: Path,
    *,
    min_score: Optional[float] = None,
    prediction_column: str = "predictions",
    positive_value: int = 1,
    score_column: str = "score",
) -> Path:
    """Convert PythonCloneDetection predictions to tool CSV format.

    Args:
        raw_csv: CSV with columns produced by scripts/run_pythonclonedetection_medium.py.
        output_csv: Target CSV file in the benchmark tool format.
        min_score: Optional minimum probability threshold (requires score column).
        prediction_column: Column with integer predictions (default: 'predictions').
        positive_value: Value in prediction_column treated as positive.
        score_column: Column name with positive-class probabilities.
    """
    raw_csv = raw_csv.resolve()
    output_csv = output_csv.resolve()

    with raw_csv.open("r", encoding="utf-8", newline="") as fin, \
         output_csv.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

        if prediction_column not in reader.fieldnames:
            raise ValueError(f"Column '{prediction_column}' not found in {raw_csv}")

        if min_score is not None and score_column not in reader.fieldnames:
            raise ValueError(
                f"Score threshold specified but column '{score_column}' is absent."
            )

        writer = csv.DictWriter(
            fout,
            fieldnames=[
                "file1_path",
                "file1_start",
                "file1_end",
                "file2_path",
                "file2_start",
                "file2_end",
            ],
        )
        writer.writeheader()

        for row in reader:
            try:
                prediction = int(row[prediction_column])
            except (TypeError, ValueError):
                continue

            if prediction != positive_value:
                continue

            if min_score is not None:
                try:
                    score = float(row[score_column])
                except (TypeError, ValueError):
                    continue
                if score < min_score:
                    continue

            try:
                writer.writerow(
                    {
                        "file1_path": row["file1_path"],
                        "file1_start": int(row["file1_start"]),
                        "file1_end": int(row["file1_end"]),
                        "file2_path": row["file2_path"],
                        "file2_start": int(row["file2_start"]),
                        "file2_end": int(row["file2_end"]),
                    }
                )
            except (TypeError, ValueError):
                continue

    return output_csv

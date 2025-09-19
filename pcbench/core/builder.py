from __future__ import annotations

import os
import csv
from pathlib import Path
from typing import Optional, Dict, List
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from .gcj_reader import read_gcj_csv
from .code_fragments import clamp_fragment, get_function_boundaries_ex
from .clone_types import classify_clone_pair
from .paths import project_root_from, to_project_relative


LANGUAGE_EXTENSIONS = {".py": "Python"}


def _language_from_filename(filename: str) -> Optional[str]:
    ext = Path(filename).suffix.lower()
    return LANGUAGE_EXTENSIONS.get(ext)


def build_benchmark(
    year: str,
    input_csv_path: Path,
    extracted_dir: Path,
    output_dir: Path,
    limit_tasks: Optional[set[str]] = None,
    granularity: str = "auto",
    min_lines: int = 5,
    shard_by_task: bool = False,
    gzip_output: bool = False,
    main_names: Optional[List[str]] = None,
    parallel_workers: int = 0,
    annotate_clone_type: bool = False,
) -> Path:
    """Строит эталонный CSV c парами клонов по GCJ данным.

    - Сохраняет Python‑решения в extracted_dir/<year>/<task>/<user>/<file>.
    - Выбирает фрагмент (function/file) по эвристикам/настройке.
    - Генерирует все попарные пары внутри одного task.
    Возвращает путь к созданному CSV.
    """
    extracted_dir_year = extracted_dir / year
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir_year.mkdir(parents=True, exist_ok=True)

    project_root = project_root_from(Path(__file__).parent.parent)

    python_by_task: Dict[str, List[dict]] = {}
    processed = 0

    # Читаем GCJ CSV
    strategy_counts: Dict[str, int] = {}
    for row in tqdm(read_gcj_csv(input_csv_path, year=year, limit_tasks=limit_tasks), desc=f"GCJ {year}"):
        language = _language_from_filename(row["file"])
        if language != "Python":
            continue

        user_dir = extracted_dir_year / row["task"] / row["username"]
        user_dir.mkdir(parents=True, exist_ok=True)

        safe_name = row["file"].replace("/", "_").replace("\\", "_")
        abs_path = user_dir / safe_name
        abs_path.write_text(row["flines"], encoding="utf-8")

        num_lines = row["flines"].count("\n") + 1

        reason = 'na'
        if granularity == "function":
            fs, fe, reason = get_function_boundaries_ex(abs_path, main_names=main_names)
        elif granularity == "file":
            fs, fe = 0, num_lines - 1
        else:  # auto
            fs, fe, reason = get_function_boundaries_ex(abs_path, main_names=main_names)

        fs, fe = clamp_fragment(fs, fe, num_lines)
        strategy_counts[reason] = strategy_counts.get(reason, 0) + 1

        # отсев по длине фрагмента
        if (fe - fs + 1) < min_lines:
            continue

        rel_path = to_project_relative(abs_path, project_root)
        python_by_task.setdefault(row["task"], []).append(
            {
                "path": rel_path,
                "start": fs,
                "end": fe,
                "granularity": "function" if granularity in ("function", "auto") else "file",
                "total_file_lines": num_lines,
            }
        )
        processed += 1

    fieldnames = [
        "file1_path",
        "file1_start",
        "file1_end",
        "file2_path",
        "file2_start",
        "file2_end",
        "granularity1",
        "granularity2",
        "task_id",
        "clone_type",
    ]

    generated = 0
    clone_type_counts: Dict[str, int] = {}

    code_cache: Dict[str, List[str]] = {}

    def _load_lines(rel_path: str) -> List[str]:
        if rel_path not in code_cache:
            abs_path = project_root / rel_path
            try:
                text = abs_path.read_text(encoding="utf-8")
            except FileNotFoundError:
                code_cache[rel_path] = []
            else:
                code_cache[rel_path] = text.splitlines()
        return code_cache[rel_path]

    def _extract_fragment(rel_path: str, start: int, end: int) -> str:
        lines = _load_lines(rel_path)
        if not lines:
            return ""
        end_idx = min(len(lines), max(0, end + 1))
        start_idx = max(0, min(start, end_idx))
        return "\n".join(lines[start_idx:end_idx])

    def open_writer(path: Path):
        if gzip_output:
            f = gzip.open(path, "wt", newline="", encoding="utf-8")
        else:
            f = path.open("w", newline="", encoding="utf-8")
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        return f, w

    if shard_by_task:
        # per-task files; можно параллелить по задачам
        def write_task(task_tuple):
            task_id, solutions = task_tuple
            n = len(solutions)
            if n < 2:
                return 0
            suffix = ".csv.gz" if gzip_output else ".csv"
            out_csv = output_dir / f"clones_{year}_{task_id}{suffix}"
            f, writer = open_writer(out_csv)
            local_count = 0
            for i in range(n):
                for j in range(i + 1, n):
                    s1, s2 = solutions[i], solutions[j]
                    clone_type = "unknown"
                    if annotate_clone_type:
                        frag1 = _extract_fragment(s1["path"], s1["start"], s1["end"])
                        frag2 = _extract_fragment(s2["path"], s2["start"], s2["end"])
                        if frag1 and frag2:
                            clone_type = classify_clone_pair(frag1, frag2)
                    clone_type_counts[clone_type] = clone_type_counts.get(clone_type, 0) + 1
                    writer.writerow(
                        {
                            "file1_path": s1["path"],
                            "file1_start": s1["start"],
                            "file1_end": s1["end"],
                            "file2_path": s2["path"],
                            "file2_start": s2["start"],
                            "file2_end": s2["end"],
                            "granularity1": s1["granularity"],
                            "granularity2": s2["granularity"],
                            "task_id": task_id,
                            "clone_type": clone_type,
                        }
                    )
                    local_count += 1
            f.close()
            return local_count

        tasks_iter = list(python_by_task.items())
        if parallel_workers and parallel_workers > 0:
            with ThreadPoolExecutor(max_workers=parallel_workers) as ex:
                for fut in tqdm(as_completed([ex.submit(write_task, t) for t in tasks_iter]), total=len(tasks_iter), desc="pairs (sharded)"):
                    generated += fut.result()
        else:
            for t in tqdm(tasks_iter, desc="pairs (sharded)"):
                generated += write_task(t)
        combined_path = output_dir / f"clones_{year}_SHARDED"
        print(f"Processed Python solutions: {processed}")
        print(f"Generated pairs: {generated}")
        if annotate_clone_type and clone_type_counts:
            print("Clone type distribution:")
            for k, v in sorted(clone_type_counts.items(), key=lambda kv: (-kv[1], kv[0])):
                pct = 100.0 * v / (sum(clone_type_counts.values()) or 1)
                print(f"  {k}: {v} ({pct:.1f}%)")
        print(f"Benchmark written (sharded by task) in {output_dir}")
        return combined_path
    else:
        out_csv = output_dir / (f"clones_{year}.csv.gz" if gzip_output else f"clones_{year}.csv")
        f, writer = open_writer(out_csv)
        for task_id, solutions in tqdm(python_by_task.items(), desc="pairs"):
            n = len(solutions)
            if n < 2:
                continue
            for i in range(n):
                for j in range(i + 1, n):
                    s1, s2 = solutions[i], solutions[j]
                    clone_type = "unknown"
                    if annotate_clone_type:
                        frag1 = _extract_fragment(s1["path"], s1["start"], s1["end"])
                        frag2 = _extract_fragment(s2["path"], s2["start"], s2["end"])
                        if frag1 and frag2:
                            clone_type = classify_clone_pair(frag1, frag2)
                    clone_type_counts[clone_type] = clone_type_counts.get(clone_type, 0) + 1
                    writer.writerow(
                        {
                            "file1_path": s1["path"],
                            "file1_start": s1["start"],
                            "file1_end": s1["end"],
                            "file2_path": s2["path"],
                            "file2_start": s2["start"],
                            "file2_end": s2["end"],
                            "granularity1": s1["granularity"],
                            "granularity2": s2["granularity"],
                            "task_id": task_id,
                            "clone_type": clone_type,
                        }
                    )
                    generated += 1
        f.close()
        # write small summary markdown
        summary = output_dir / f"benchmark_{year}_summary.md"
        with summary.open("w", encoding="utf-8") as sf:
            sf.write("# Benchmark Summary\n\n")
            sf.write(f"- Year: {year}\n")
            sf.write(f"- Python solutions: {processed}\n")
            sf.write(f"- Generated pairs: {generated}\n")
            sf.write(f"- Output: {out_csv.name}\n")
            sf.write(f"- Granularity: {granularity}\n")
            sf.write(f"- Min lines: {min_lines}\n")
            if annotate_clone_type and clone_type_counts:
                total_types = sum(clone_type_counts.values()) or 1
                sf.write("\n## Clone type distribution\n")
                for k, v in sorted(clone_type_counts.items(), key=lambda kv: (-kv[1], kv[0])):
                    pct = 100.0 * v / total_types
                    sf.write(f"- {k}: {v} ({pct:.1f}%)\n")
            sf.write("\n## Fragment selection strategies\n")
            total_sel = sum(strategy_counts.values()) or 1
            for k, v in sorted(strategy_counts.items(), key=lambda kv: (-kv[1], kv[0])):
                pct = 100.0 * v / total_sel
                sf.write(f"- {k}: {v} ({pct:.1f}%)\n")
        print(f"Processed Python solutions: {processed}")
        print(f"Generated pairs: {generated}")
        if annotate_clone_type and clone_type_counts:
            print("Clone type distribution:")
            for k, v in sorted(clone_type_counts.items(), key=lambda kv: (-kv[1], kv[0])):
                pct = 100.0 * v / (sum(clone_type_counts.values()) or 1)
                print(f"  {k}: {v} ({pct:.1f}%)")
        print(f"Benchmark written: {out_csv}")
        return out_csv

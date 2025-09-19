from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple
import gzip


def _open(path: Path):
    return gzip.open(path, "rt", encoding="utf-8", newline="") if str(path).endswith('.gz') else path.open("r", encoding="utf-8", newline="")


def _detect_root(benchmark_csv: Path, project_root: Path) -> Path:
    """Пытается угадать корректный корень проекта.
    Приоритет: заданный project_root; рядом с модулем; родители benchmark_csv;
    выбираем тот, где существует подкаталог extracted_solutions.
    """
    candidates = [
        project_root,
        Path(__file__).resolve().parents[1],
    ] + list(benchmark_csv.resolve().parents)

    for cand in candidates:
        es = cand / 'extracted_solutions'
        if es.exists() and es.is_dir():
            return cand.resolve()
    return project_root.resolve()


def qc_benchmark(benchmark_csv: Path, project_root: Path) -> Dict[str, int | float]:
    """Проверяет валидность путей/координат и собирает базовую статистику.

    Возвращает словарь с количеством строк, валидных/невалидных записей, минимум/максимум/средний размер фрагмента и т.п.
    """
    total = 0
    ok = 0
    bad_path = 0
    bad_coords = 0
    min_len = 10**9
    max_len = 0
    sum_len = 0

    root = _detect_root(benchmark_csv, project_root)

    with _open(benchmark_csv) as f:
        r = csv.DictReader(f)
        for row in r:
            total += 1
            try:
                p1 = root / row['file1_path']
                p2 = root / row['file2_path']
                if not p1.exists() or not p2.exists():
                    bad_path += 1
                    continue
                s1 = int(row['file1_start']); e1 = int(row['file1_end'])
                s2 = int(row['file2_start']); e2 = int(row['file2_end'])
                if not (0 <= s1 <= e1) or not (0 <= s2 <= e2):
                    bad_coords += 1
                    continue
                l1 = e1 - s1 + 1
                l2 = e2 - s2 + 1
                frag_len = (l1 + l2) / 2
                min_len = min(min_len, frag_len)
                max_len = max(max_len, frag_len)
                sum_len += frag_len
                ok += 1
            except Exception:
                bad_coords += 1
                continue

    avg_len = (sum_len / ok) if ok else 0.0
    return {
        'rows': total,
        'ok': ok,
        'bad_path': bad_path,
        'bad_coords': bad_coords,
        'min_len': 0 if min_len == 10**9 else int(min_len),
        'max_len': int(max_len),
        'avg_len': float(f"{avg_len:.2f}"),
    }


def write_qc_report(report_dir: Path, summary: Dict[str, int | float]):
    report_dir.mkdir(parents=True, exist_ok=True)
    md = report_dir / 'benchmark_qc_summary.md'
    with md.open('w', encoding='utf-8') as f:
        f.write('# Benchmark QC Summary\n\n')
        for k, v in summary.items():
            f.write(f'- {k}: {v}\n')
    return md

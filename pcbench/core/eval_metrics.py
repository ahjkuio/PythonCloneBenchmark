from __future__ import annotations


def get_line_count(start: int, end: int) -> int:
    if start < 0 or end < 0 or end < start:
        return 0
    return end - start + 1


def calculate_fragment_coverage(b_start: int, b_end: int, t_start: int, t_end: int) -> tuple[float, float]:
    lines_b = get_line_count(b_start, b_end)
    lines_t = get_line_count(t_start, t_end)
    if lines_b == 0 and lines_t == 0:
        return 1.0, 1.0
    if lines_b == 0 or lines_t == 0:
        return 0.0, 0.0
    overlap_start = max(b_start, t_start)
    overlap_end = min(b_end, t_end)
    overlap = get_line_count(overlap_start, overlap_end)
    cov_b_by_t = overlap / lines_b
    cov_t_by_b = overlap / lines_t
    return cov_b_by_t, cov_t_by_b

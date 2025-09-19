from pcbench.core.eval_metrics import get_line_count, calculate_fragment_coverage


def test_get_line_count():
    assert get_line_count(0, 0) == 1
    assert get_line_count(0, 9) == 10
    assert get_line_count(5, 4) == 0


def test_coverage():
    b_by_t, t_by_b = calculate_fragment_coverage(0, 9, 5, 14)
    assert abs(b_by_t - 0.5) < 1e-9
    assert abs(t_by_b - 0.5) < 1e-9

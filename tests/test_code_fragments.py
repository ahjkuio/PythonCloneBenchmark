import tempfile
from pathlib import Path
import sys

# Ensure project root (PythonCloneBenchmark_new) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.core.code_fragments import get_function_boundaries, clamp_fragment, get_function_boundaries_ex


def _write(tmp: Path, text: str) -> Path:
    p = tmp / "sample.py"
    p.write_text(text, encoding="utf-8")
    return p


def test_function_boundaries_simple():
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), """
def solve():
    x = 1
    return x
""".lstrip())
        s, e = get_function_boundaries(p)
        assert s is not None and e is not None
        assert s <= e


def test_function_boundaries_no_funcs():
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), """
# just script
print(1)
""".lstrip())
        s, e = get_function_boundaries(p)
        assert s == 1  # первая значимая после комментария
        assert e >= s


def test_clamp_fragment():
    s, e = clamp_fragment(None, None, 10)
    assert (s, e) == (0, 9)
    s, e = clamp_fragment(5, 100, 10)
    assert (s, e) == (5, 9)


def test_main_guard_calls_function():
    code = """
def helper():
    pass

def do():
    a = 1
    a += 2
    return a

if __name__ == "__main__":
    do()
""".lstrip()
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), code)
        s, e, reason = get_function_boundaries_ex(p)
        lines = p.read_text(encoding="utf-8").splitlines()
        assert lines[s].startswith("def do(")
        assert lines[e].strip() == "return a"
        assert reason in {"main_call_func", "main_call_func_alias", "ast_named"}


def test_main_guard_calls_class_method():
    code = """
class Solver:
    def __init__(self):
        pass
    def run(self):
        x = 1
        return x

if __name__ == "__main__":
    Solver().run()
""".lstrip()
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), code)
        s, e, reason = get_function_boundaries_ex(p)
        lines = p.read_text(encoding="utf-8").splitlines()
        assert lines[s].strip().startswith("def run(")
        assert lines[e].strip() == "return x"
        assert reason in {"main_call_method", "ast_named"}


def test_main_guard_with_instance_variable():
    code = """
class Solver:
    def run(self):
        return 42


if __name__ == "__main__":
    solver = Solver()
    solver.run()
""".lstrip()
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), code)
        s, e, reason = get_function_boundaries_ex(p)
        lines = p.read_text(encoding="utf-8").splitlines()
        assert lines[s].strip().startswith("def run(")
        assert reason in {"main_call_method", "ast_named"}


def test_alias_main_points_to_solver():
    code = """
def solve():
    return 1

main = solve

if __name__ == "__main__":
    main()
""".lstrip()
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), code)
        s, e, reason = get_function_boundaries_ex(p)
        lines = p.read_text(encoding="utf-8").splitlines()
        assert lines[s].startswith("def solve(")
        assert reason in {"main_call_func_alias", "ast_alias", "ast_named"}


def test_async_def_is_detected():
    code = """
async def solve():
    return 1


def helper():
    return 2
""".lstrip()
    with tempfile.TemporaryDirectory() as d:
        p = _write(Path(d), code)
        s, e, reason = get_function_boundaries_ex(p)
        lines = p.read_text(encoding="utf-8").splitlines()
        assert lines[s].startswith("async def solve(")
        assert reason in {"ast_named", "ast_alias", "regex_named"}

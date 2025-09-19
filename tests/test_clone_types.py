from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pcbench.core.clone_types import classify_clone_pair, CloneTypeThresholds


def test_type1_exact_ignores_whitespace_and_comments():
    code_a = """
# comment
  def solve():
      return 1
""".strip()
    code_b = """
def solve():
    return 1  # inline comment
""".strip()
    assert classify_clone_pair(code_a, code_b) == "type1"


def test_type2_when_identifiers_renamed():
    code_a = """
def solve(x):
    total = x + 1
    return total
""".strip()
    code_b = """
def solve(value):
    tmp = value + 1
    return tmp
""".strip()
    assert classify_clone_pair(code_a, code_b) == "type2"


def test_type3_similarity_threshold_controls_type():
    code_a = """
def solve(n):
    result = 0
    for i in range(n):
        result += i
    return result
""".strip()
    code_b = """
def solve(n):
    result = 0
    for i in range(n):
        result += i * 2
    result //= 2
    return result
""".strip()
    thresholds = CloneTypeThresholds(type3_min_similarity=0.4)
    assert classify_clone_pair(code_a, code_b, thresholds=thresholds) == "type3"


def test_type4_when_semantics_only():
    code_a = """
def solve():
    return sum(range(10))
""".strip()
    code_b = """
def solve():
    from math import factorial
    return factorial(5)
""".strip()
    assert classify_clone_pair(code_a, code_b) == "type4"

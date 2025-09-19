from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Optional, List


MAIN_CANDIDATE_NAMES = [
    "solve",
    "main",
    "solution",
    "run",
    "process",
    "task",
    "go",
    "start",
]


def _is_main_guard(test: ast.AST) -> bool:
    """Проверяет, что if-test — это __name__ == "__main__" (или в обратном порядке)."""
    # Варианты: Compare(left=Name("__name__"), ops=[Eq()], comparators=[Constant("__main__")])
    # или Compare(left=Constant("__main__"), ops=[Eq()], comparators=[Name("__name__")])
    if not isinstance(test, ast.Compare) or len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
        return False
    left, right = test.left, test.comparators[0]
    def is_name(node, s):
        return isinstance(node, ast.Name) and node.id == s
    def is_str_main(node):
        return (isinstance(node, ast.Constant) and node.value == "__main__") or (
            hasattr(ast, "Str") and isinstance(node, ast.Str) and node.s == "__main__"
        )
    return (is_name(left, "__name__") and is_str_main(right)) or (is_str_main(left) and is_name(right, "__name__"))


def _collect_ast_info(tree: ast.AST):
    """Собирает top-level функции и методы классов: возвращает два словаря.

    functions: name -> (start, end)
    methods: (class_name, method_name) -> (start, end)
    Все координаты 0-based, включительно. Если отсутствует end_lineno — пропускаем.
    """
    functions: dict[str, tuple[int, int]] = {}
    methods: dict[str, dict[str, tuple[int, int]]] = {}

    def _record(target: dict, name: str, node: ast.AST) -> None:
        end = getattr(node, "end_lineno", None)
        if end is None:
            return
        target[name] = (node.lineno - 1, end - 1)

    for node in getattr(tree, "body", []):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            _record(functions, node.name, node)
        elif isinstance(node, ast.ClassDef):
            cls_methods = methods.setdefault(node.name, {})
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    _record(cls_methods, sub.name, sub)

    flat_methods = {(cls, m): span for cls, entries in methods.items() for m, span in entries.items()}
    return functions, flat_methods


def _find_main_invocation(tree: ast.AST):
    """Ищет в блоке if __name__ == "__main__" вызов функции или метода.

    Возвращает:
      ("func", func_name) или ("method", class_name, method_name) или None.
    Обрабатывает случаи вида solve(), run(), Solver().run().
    """
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.If) and _is_main_guard(node.test):
            instance_map: dict[str, str] = {}
            for stmt in node.body:
                if isinstance(stmt, ast.Assign):
                    if len(stmt.targets) != 1:
                        continue
                    target = stmt.targets[0]
                    if isinstance(target, ast.Name) and isinstance(stmt.value, ast.Call):
                        call_func = stmt.value.func
                        if isinstance(call_func, ast.Name):
                            instance_map[target.id] = call_func.id
                if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                    call = stmt.value
                    func = call.func
                    if isinstance(func, ast.Name):
                        return ("func", func.id)
                    if isinstance(func, ast.Attribute):
                        attr = func
                        base = attr.value
                        if isinstance(base, ast.Call) and isinstance(base.func, ast.Name):
                            return ("method", base.func.id, attr.attr)
                        if isinstance(base, ast.Name) and base.id in instance_map:
                            return ("method", instance_map[base.id], attr.attr)
    return None


def _collect_function_aliases(tree: ast.AST) -> dict[str, str]:
    """Возвращает отображение имя->оригинальная функция."""
    aliases: dict[str, str] = {}
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                value = node.value
                if isinstance(value, ast.Name):
                    aliases[target.id] = value.id
    return aliases


def get_function_boundaries(file_path: Path, main_names: Optional[List[str]] = None) -> tuple[int | None, int | None]:
    """Возвращает только координаты, используя расширенный вариант ниже."""
    start, end, _ = get_function_boundaries_ex(file_path, main_names)
    return start, end


def clamp_fragment(start: int | None, end: int | None, total_lines: int) -> tuple[int, int]:
    """Гарантирует валидные координаты внутри файла (0..total_lines-1)."""
    if start is None or end is None:
        return 0, max(0, total_lines - 1)
    s = max(0, min(start, total_lines - 1))
    e = max(0, min(end, total_lines - 1))
    if e < s:
        s, e = 0, max(0, total_lines - 1)
    return s, e


def get_function_boundaries_ex(file_path: Path, main_names: Optional[List[str]] = None) -> tuple[int | None, int | None, str]:
    """Как get_function_boundaries, но с пометкой стратегии выбора.

    Возможные значения reason:
      - 'main_call_func' / 'main_call_func_alias' / 'main_call_method'
      - 'ast_named' / 'ast_alias' / 'ast_single' / 'ast_longest'
      - 'regex_named' / 'regex_single' / 'regex_longest'
      - 'script_trim' / 'fullfile_fallback' / 'error_default'
    """
    try:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        if not lines:
            return None, None, 'empty'

        try:
            tree = ast.parse(text, filename=file_path.name)
            functions, methods = _collect_ast_info(tree)
            aliases = _collect_function_aliases(tree)
            main_list = [n.lower() for n in (main_names or MAIN_CANDIDATE_NAMES)]

            main_call = _find_main_invocation(tree)
            if main_call:
                if main_call[0] == "func":
                    original = aliases.get(main_call[1], main_call[1])
                    if original in functions:
                        s, e = functions[original]
                        reason = 'main_call_func_alias' if original != main_call[1] else 'main_call_func'
                        return s, e, reason
                elif main_call[0] == "method":
                    cls, mname = main_call[1], main_call[2]
                    if (cls, mname) in methods:
                        s, e = methods[(cls, mname)]
                        return s, e, 'main_call_method'

            if functions:
                for name in functions:
                    if name.lower() in main_list:
                        s, e = functions[name]
                        return s, e, 'ast_named'
                for alias, original in aliases.items():
                    if alias.lower() in main_list and original in functions:
                        s, e = functions[original]
                        return s, e, 'ast_alias'
                if len(functions) == 1:
                    (name, (s, e)) = next(iter(functions.items()))
                    return s, e, 'ast_single'
                name, (s, e) = max(functions.items(), key=lambda kv: kv[1][1] - kv[1][0])
                return s, e, 'ast_longest'
        except Exception:
            pass

        pattern = re.compile(r'^(\s*)(?:async\s+)?def\s+(\w+)\s*\(')
        functions_found = []
        for i, line in enumerate(lines):
            m = pattern.match(line)
            if not m:
                continue
            name = m.group(2)
            start = i
            current_indent = len(m.group(1))
            end = len(lines) - 1
            for j in range(i + 1, len(lines)):
                stripped = lines[j].lstrip()
                if stripped and not stripped.startswith('#'):
                    indent = len(lines[j]) - len(stripped)
                    if indent <= current_indent:
                        end = j - 1
                        break
            functions_found.append((name, start, end))

        if functions_found:
            main_list = [n.lower() for n in (main_names or MAIN_CANDIDATE_NAMES)]
            for name, s, e in functions_found:
                if name.lower() in main_list:
                    return s, e, 'regex_named'
            if len(functions_found) == 1:
                return functions_found[0][1], functions_found[0][2], 'regex_single'
            name, s, e = max(functions_found, key=lambda x: x[2] - x[1])
            return s, e, 'regex_longest'

        # Script trim
        start_line = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#') and not stripped.startswith('import') and not stripped.startswith('from'):
                start_line = i
                break
        return start_line, max(0, len(lines) - 1), 'script_trim'

    except Exception:
        try:
            n = sum(1 for _ in file_path.open('r', encoding='utf-8', errors='ignore'))
            return 0, max(0, n - 1), 'fullfile_fallback'
        except Exception:
            return 0, 100, 'error_default'

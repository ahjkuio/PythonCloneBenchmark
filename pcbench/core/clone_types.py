from __future__ import annotations

import keyword
import tokenize
from dataclasses import dataclass
from difflib import SequenceMatcher
from io import StringIO
from typing import Iterable


@dataclass(frozen=True)
class CloneTypeThresholds:
    """Thresholds for distinguishing near-miss (Type-3) from Type-4."""

    type3_min_similarity: float = 0.8


def _token_stream(code: str) -> Iterable[tokenize.TokenInfo]:
    reader = StringIO(code).readline
    try:
        for tok in tokenize.generate_tokens(reader):
            yield tok
    except tokenize.TokenError:
        return


def _normalize_tokens(code: str, *, structural: bool) -> str:
    parts: list[str] = []
    for tok in _token_stream(code):
        ttype = tok.type
        if ttype in (tokenize.COMMENT, tokenize.NL, tokenize.ENCODING):
            continue
        if ttype in (tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT):
            continue
        text = tok.string
        if structural:
            if ttype == tokenize.NAME and not keyword.iskeyword(text):
                parts.append("ID")
                continue
            if ttype == tokenize.NUMBER:
                parts.append("NUM")
                continue
            if ttype == tokenize.STRING:
                parts.append("STR")
                continue
        parts.append(text)
    return " ".join(parts)


def classify_clone_pair(code1: str, code2: str, *, thresholds: CloneTypeThresholds | None = None) -> str:
    """Classify a pair of Python fragments into Roy/Svajlenko Type-1..4."""
    thresholds = thresholds or CloneTypeThresholds()

    lexical1 = _normalize_tokens(code1, structural=False)
    lexical2 = _normalize_tokens(code2, structural=False)
    if lexical1 == lexical2:
        return "type1"

    structural1 = _normalize_tokens(code1, structural=True)
    structural2 = _normalize_tokens(code2, structural=True)
    if structural1 == structural2:
        return "type2"

    if structural1 and structural2:
        similarity = SequenceMatcher(None, structural1, structural2).ratio()
        if similarity >= thresholds.type3_min_similarity:
            return "type3"

    return "type4"


__all__ = [
    "CloneTypeThresholds",
    "classify_clone_pair",
]

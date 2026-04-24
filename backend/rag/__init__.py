"""
`rag` package

This package contains the RAG pipeline implementation plus hardening modules.

Design note:
  - Keep package import lightweight so `rag.chunk_sanitiser` can be imported
    without importing DB/LLM dependencies.
  - Export `query`, `rebuild_index`, and `get_departments` via lazy attribute
    loading from `rag.core`.
"""

from __future__ import annotations

from typing import Any


__all__ = ["query", "rebuild_index", "get_departments"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from . import core

        return getattr(core, name)
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)


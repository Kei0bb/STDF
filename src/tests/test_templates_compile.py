"""All analysis cell-scripts must compile (they are run, not imported)."""

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]   # repo root: src/tests → repo
_TEMPLATES = sorted((_ROOT / "templates" / "analysis").glob("*.py"))


@pytest.mark.parametrize("path", _TEMPLATES, ids=lambda p: p.name)
def test_template_compiles(path):
    src = path.read_text(encoding="utf-8")
    compile(src, str(path), "exec")   # SyntaxError fails the test


def test_four_templates_present():
    assert len(_TEMPLATES) == 4

import ast
import re
import sys
import sysconfig
import tomllib
from collections.abc import Iterator
from pathlib import Path

import pytest
import spillsort

ROOT = Path(__file__).parent.parent


def test_readme_links_work_on_pypi() -> None:
    targets = re.findall(r"\]\(([^)]+)\)", (ROOT / "README.md").read_text())
    assert targets
    assert all(target.startswith("https://") for target in targets), targets


def test_the_version_comes_from_cargo() -> None:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text())
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    assert "version" not in project
    assert project["dynamic"] == ["version"]
    assert spillsort.__version__ == cargo["package"]["version"]


def test_the_public_names_are_exactly_those_exported() -> None:
    public = {name for name in vars(spillsort) if not name.startswith("_")}
    assert public == set(spillsort.__all__)
    assert spillsort.Sorter.__module__ == "spillsort"


def test_the_stub_exports_only_public_names() -> None:
    stub = ast.parse((ROOT / "python" / "spillsort" / "_spillsort.pyi").read_text())
    (exported,) = [
        ast.literal_eval(node.value)
        for node in stub.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
    ]
    assert set(exported) <= set(spillsort.__all__)


def test_sorting_returns_an_iterator_of_a_private_type() -> None:
    iterator = spillsort.Sorter[int]().sort()
    assert isinstance(iterator, Iterator)
    assert type(iterator).__name__.startswith("_")


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="Python is not free-threaded"
)
def test_importing_keeps_the_gil_disabled() -> None:
    assert not getattr(sys, "_is_gil_enabled")()  # noqa: B009

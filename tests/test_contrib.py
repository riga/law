from __future__ import annotations

__all__ = ["TestContrib"]

import ast
import os

from law.util import law_src_path


def contrib_packages() -> list[str]:
    # same criterion as used by law.contrib.load
    contrib_dir = law_src_path("contrib")
    return sorted(
        name for name in os.listdir(contrib_dir)
        if os.path.isfile(os.path.join(contrib_dir, name, "__init__.py"))
    )


def type_checking_contrib_imports() -> list[tuple[str, str | None]]:
    with open(law_src_path("__init__.py"), encoding="utf-8") as f:
        tree = ast.parse(f.read())

    imports: list[tuple[str, str | None]] = []
    for node in tree.body:
        if not isinstance(node, ast.If) or getattr(node.test, "id", None) != "TYPE_CHECKING":
            continue
        for stmt in node.body:
            if isinstance(stmt, ast.ImportFrom) and stmt.module == "law.contrib":
                imports.extend((alias.name, alias.asname) for alias in stmt.names)

    return imports


class TestContrib:

    def test_type_checking_imports(self) -> None:
        # all contrib packages must be imported for type checkers in law/__init__.py, with explicit re-exports
        imports = type_checking_contrib_imports()
        assert sorted(name for name, _ in imports) == contrib_packages()
        assert all(name == asname for name, asname in imports)

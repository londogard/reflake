"""Worktree ignore rules: ``.reflakeignore`` plus built-in defaults.

Ignore evaluation is deliberately small and predictable:

- one pattern per line, ``#`` comments, blank lines skipped;
- a leading ``!`` re-includes previously ignored paths;
- a trailing ``/`` matches directories only (so a matched directory is
  pruned whole, git-style);
- a pattern containing ``/`` is anchored to the tree root, one without
  matches any path component;
- ``*`` and ``?`` do not cross ``/``; ``**`` does; ``[abc]`` classes work.

Built-in defaults (``.reflake``, ``.git/``) can be re-included with ``!``.
This is intentionally *not* a full gitignore implementation; it is the
subset needed so a dataset repository never swallows its own tooling.
"""

from __future__ import annotations

import re
from pathlib import Path

#: Applied before user rules so a negation can re-include them.
DEFAULT_PATTERNS: tuple[str, ...] = (".reflake/", ".git/")

IGNORE_FILENAME = ".reflakeignore"


def _translate(pattern: str) -> str:
    """Translate a gitignore-style glob into a regex fragment."""
    out: list[str] = []
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                index += 1
                if index + 1 < len(pattern) and pattern[index + 1] == "/":
                    # ``**/`` matches zero or more directories.
                    out.append("(?:.*/)?")
                    index += 1
                else:
                    out.append(".*")
            else:
                out.append("[^/]*")
        elif char == "?":
            out.append("[^/]")
        elif char == "[":
            end = pattern.find("]", index + 1)
            if end == -1:
                out.append(re.escape(char))
            else:
                inner = pattern[index + 1 : end].replace("\\", "\\\\")
                if inner.startswith("!"):
                    inner = "^" + inner[1:]
                out.append(f"[{inner}]")
                index = end
        else:
            out.append(re.escape(char))
        index += 1
    return "".join(out)


class IgnoreRules:
    """Compiled ignore rules for one tree root."""

    def __init__(self, patterns: tuple[str, ...] | list[str]) -> None:
        self._rules: list[tuple[bool, bool, re.Pattern[str]]] = []
        for raw in patterns:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            negated = line.startswith("!")
            if negated:
                line = line[1:].strip()
                if not line:
                    continue
            dir_only = line.endswith("/")
            line = line.rstrip("/")
            if not line:
                continue
            anchored = line.startswith("/")
            if anchored:
                line = line.lstrip("/")
            if not anchored:
                anchored = "/" in line
            body = _translate(line)
            expression = f"^{body}$" if anchored else f"(?:^|/){body}$"
            self._rules.append((negated, dir_only, re.compile(expression)))

    @classmethod
    def load(cls, tree_root: str | Path) -> IgnoreRules:
        """Load ``<tree_root>/.reflakeignore`` over the built-in defaults."""
        patterns: list[str] = list(DEFAULT_PATTERNS)
        path = Path(tree_root) / IGNORE_FILENAME
        try:
            if path.is_file():
                patterns.extend(path.read_text(encoding="utf-8").splitlines())
        except OSError:
            pass
        return cls(patterns)

    def ignores(self, relative_path: str, *, is_dir: bool) -> bool:
        """Last matching rule wins (git semantics for simple patterns)."""
        ignored = False
        for negated, dir_only, pattern in self._rules:
            if dir_only and not is_dir:
                continue
            if pattern.search(relative_path) is not None:
                ignored = not negated
        return ignored

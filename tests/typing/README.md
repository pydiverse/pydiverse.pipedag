# Type checker assertions for the public API

The files in this directory are not pytest tests. They are inputs for pyright and mypy, which are
configured in `pyproject.toml` to check exactly this directory. Their job is to pin down what
pipedag's public annotations promise:

- `positive.py` — everything that must check cleanly. Uses `typing.assert_type` to state the exact
  inferred types, so a change that widens or narrows them fails the check.
- `negative.py` — everything that must be reported as an error. Each line carries both a
  `# type: ignore[...]` and a `# pyright: ignore[...]` marker. Both checkers are configured to
  report unused ignores, so if a regression makes one of these errors *disappear*, the check fails
  too.

Run them with:

```shell
pixi run -e typecheck pyright     # authoritative
pixi run -e typecheck mypy        # second opinion
```

The files are also collected by pytest (they import cleanly and define module-level flows lazily
inside functions, so importing them has no side effects) but they contain no test functions.

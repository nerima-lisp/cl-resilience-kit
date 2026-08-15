# Contributing

## Design constraints

- Keep resilience policy decisions explicit. Do not add process-global
  breakers, limiters, timers, or worker threads; clocks, random sources,
  sleepers, cancellation tokens, event handlers, state stores, and lease
  stores must stay injectable at the operation boundary.
- Preserve injectable boundaries when extending an existing control or
  adding a new one, so tests can supply deterministic clocks and stores
  instead of depending on wall-clock time or process-global state.
- Add focused tests for new behavior, and run the narrowest relevant check
  before the full Nix check.

## Development environment

The supported path is the Nix development shell:

```sh
nix develop
```

From the repository root:

```sh
nix run .#test              # run the test suite
nix build .#coverage         # build the coverage report
nix build .#docs             # build the MkDocs documentation site
nix flake check              # run every check: build, tests, coverage, paredit lint
nix fmt                      # format the tree
paredit inspect lint src t --fail-on error   # structural lint only
```

### The canonical gate is `nix flake check`

`nix flake check` is the check that must pass before a change is considered
done. It runs the package build, the test suite, the coverage check, and the
structural `paredit-lint` check together.

**A green `paredit inspect lint` does not mean the code compiles.** This
repository has already shipped a commit where `paredit inspect lint` reported
`99 files parsed, 0 parse errors` while the library failed to compile: the
defect was parenthesis-balanced but semantically malformed, so the structural
linter had nothing to flag. Treat a passing `paredit` lint as evidence that
the source is well-formed s-expressions, not as evidence that it builds or
behaves correctly. Always confirm with `nix flake check` (or, at minimum,
`nix build .#cl-resilience-kit` and `nix run .#test`) before treating a
change as verified.

### Direct SBCL runs

The repository also documents a direct test invocation outside the Nix
shell:

```sh
sbcl --non-interactive --no-userinit --no-sysinit \
  --load run-tests.lisp
```

This path has real preconditions:

- It depends on the Nerima Lisp packages `cl-boundary-kit`,
  `cl-concurrent-kit`, `cl-date-kit`, and (for the full test suite)
  `cl-dataflow`, `cl-observability-kit`, `cl-prolog`, and `cl-weave`. None of
  these are published on Quicklisp.
- `scripts/bootstrap.lisp` resolves them either from adjacent nerima-lisp
  checkouts at `../<name>/` next to this repository, or from a ghq
  bare-clone layout (`<name>.git/` three directories up), materializing
  missing sibling sources into a temporary tree for the direct run.
- **This path currently hangs on macOS/aarch64 with SBCL 2.6.0**, for both
  `run-tests.lisp` and `run-coverage.lisp`. The hang is not caused by
  anything in this repository: a bare `require :asdf` plus a list of the
  same directories reproduces the stall with no project code loaded. SBCL's
  garbage collector worker threads park in `semaphore_wait_trap` and make no
  further progress. The cause is unresolved at the SBCL level, and there is
  no known workaround — enlarging SBCL's heap only changes how far the run
  gets before hitting the same blocking site, it does not avoid it. `nix
  flake check` remains the canonical gate; treat the direct SBCL invocation
  as unusable on this platform until SBCL resolves the underlying issue.

## Before opening a change

1. Run the narrowest relevant check first: targeted tests for source
   changes, `nix build .#docs` plus `git diff --check` for
   documentation-only changes, or `paredit inspect lint src t --fail-on
   error` for structural source changes.
2. Run `nix flake check` before proposing the change as complete. A passing
   narrower check is not a substitute for the full gate.
3. Keep tests focused on the behavior being added or fixed, following the
   existing test suite's use of cl-weave property and fuzz tests, polling
   assertions, continuation-value assertions, and mocked injectable
   boundaries.

## Reporting issues

Open an issue in the
[GitHub repository](https://github.com/nerima-lisp/cl-resilience-kit) with a
minimal reproduction and the command that exposed the problem.

# Development

## Nix workflow

From the repository root:

```sh
nix develop
nix run .#test
nix build .#coverage
nix build .#docs
nix flake check
nix fmt
paredit inspect lint src t --fail-on error
```

`nix run .#test` runs the non-empty test selection. The flake test check uses
a 120-second execution limit; the coverage check uses a 300-second limit.
`nix build .#docs` evaluates the MkDocs site defined in `docs/mkdocs.yml`.
`nix flake check` also evaluates the pinned Nerima Lisp `paredit-cli` package
through the `paredit-lint` check, and the development shell exposes that tool
as the `paredit` command.

To build the installable ASDF package and repeat the same build in place:

```sh
nix build .#cl-resilience-kit
nix build .#cl-resilience-kit --rebuild
```

The package and test derivations compile from a build-private writable path so
SBCL's absolute source names in FASLs do not depend on Nix's ephemeral build
directory or collide with another derivation. Nix supplies the dependency
registry for those derivations. The direct test and coverage runners load
`scripts/bootstrap.lisp`, which registers the project and available adjacent
NERIMA checkouts for local development.

## Direct SBCL checks

When working outside the development shell, the repository also documents the
direct test commands:

```sh
sbcl --dynamic-space-size 8192 --non-interactive --no-userinit --no-sysinit \
  --load run-tests.lisp
```

The larger dynamic space works around an SBCL garbage-collector stall
observed during ASDF's source-registry scan on some platforms; it is not a
fix in this repository, and the direct run has not been confirmed to
complete under it. The bootstrap script accepts either adjacent nerima-lisp
checkouts or the shared ghq bare-clone layout and materializes missing
sibling sources automatically for the direct run.

For a coverage report, provide an output directory or let the script use its
default:

```sh
sbcl --script run-coverage.lisp /tmp/cl-resilience-kit-coverage
```

The test runner rejects an empty selection and reports the number of selected
tests and result events. Coverage describes the selected test run; it is not a
substitute for assertions or behavioral checks. Use `nix flake check
--all-systems` when the change needs evaluation on every declared platform.

The test suite uses cl-weave property and fuzz tests, polling assertions,
continuation-aware assertions, mocked boundaries, and
explicit assertion-count contracts. The direct runner applies a 30-second
per-test timeout. The Nix test derivation keeps a 120-second process-level
limit for build and startup overhead; coverage keeps a 300-second limit.

## Documentation layout

The documentation source follows the nerima-lisp layout:

```text
docs/
├── mkdocs.yml
└── src/
    ├── index.md
    ├── getting-started.md
    ├── guide/
    ├── reference/
    └── project/
```

The MkDocs configuration uses Material, places the source under `docs/src`,
and keeps navigation grouped into Guide, Reference, and Project sections.

## Before opening a change

Run the narrowest relevant check first, then the Nix check that exercises the
changed output. For documentation-only changes, build `.#docs`, inspect the
generated output, and run `git diff --check`. For source changes, run the
targeted tests before the full flake check. For structural source changes,
run `paredit inspect lint src t --fail-on error` before the broader Nix
checks.

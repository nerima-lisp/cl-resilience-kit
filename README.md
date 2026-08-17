# cl-resilience-kit

`cl-resilience-kit` provides composable resilience primitives for Common Lisp
operations that can fail transiently: service calls, database access, queues,
filesystem operations, and other application boundaries. It builds directly on
the Nerima Lisp packages `cl-boundary-kit`, `cl-concurrent-kit`, and
`cl-date-kit`, while staying independent of any specific HTTP, PostgreSQL,
Redis, or other protocol package.

The full guide is available at
[nerima-lisp.github.io/cl-resilience-kit](https://nerima-lisp.github.io/cl-resilience-kit/),
with its source in [`docs/src`](docs/src/).

## Quick start

Retry safety is explicit and belongs to the caller:

```common-lisp
(in-package #:resilience-kit)

(define-condition temporary-storage-error (error) ())

(let ((attempts 0)
      (policy
        (make-retry-policy
         :max-attempts 3
         :initial-delay 0.05d0
         :jitter :equal
         :retry-safe-p t
         :condition-classifier
         (lambda (condition attempt)
           (declare (ignore attempt))
           (typep condition 'temporary-storage-error)))))
  (call-with-retry
   policy
   (lambda ()
     (incf attempts)
     (if (< attempts 3)
         (error 'temporary-storage-error)
         :ok))))
;; => :OK
```

`max-attempts` includes the initial call. A classifier should identify only
failures that the application can safely repeat.

## Install

### ASDF

Make the repository available to ASDF's source registry, then load:

```common-lisp
(asdf:load-system :cl-resilience-kit)
```

The production system uses the Nerima Lisp packages `cl-boundary-kit` for
injectable effects, `cl-concurrent-kit` for synchronization, and `cl-date-kit`
for executor timing. The optional `cl-resilience-kit/observability` system
adds direct `cl-observability-kit` metrics, and the optional
`cl-resilience-kit/dataflow` system keeps the Nerima Lisp `cl-dataflow-kit` API
available from `resilience-dataflow` while adding the same resilience controls
through a `define-resilience-pipeline` macro that matches `cl-dataflow-kit`
syntax; the test system alone depends on `cl-weave`. The development shell
also pins the Nerima Lisp `paredit-cli` package and exposes it as the
`paredit` command for structural linting.

Load the ASDF systems by their `cl-...` names, then use the Nerima Lisp
package names in code: `resilience-kit`, `resilience-observability`, and
`resilience-dataflow`. The `cl-...` forms remain available as compatibility
nicknames.

```common-lisp
(asdf:load-system :cl-resilience-kit)
(in-package #:resilience-kit)
```

### Nix

Pin the flake in a consuming project:

```nix
{
  inputs.cl-resilience-kit.url = "github:nerima-lisp/cl-resilience-kit";
}
```

## Features

- retry policies with exponential backoff, delay caps, jitter, deadlines,
  cancellation, fallbacks, and shared retry budgets;
- local and distributed circuit breakers with half-open probe limits,
  compare-and-swap state, and optional fencing leases;
- non-blocking and queued bulkheads plus a monotonic token-bucket rate limiter;
- explicit bounded executors, best-effort worker hard-timeouts, delayed
  hedging, and process-local request coalescing;
- transport-neutral context with correlation, tracing, idempotency, tags, and
  baggage fields;
- structured events, metrics, observers, lifecycle draining, and health
  registries;
- explicit composition through `call-with-resilience`, `with-resilience`, and
  continuation-oriented `call-with-resilience/k` / `with-resilience/k` APIs;
  the latter preserve multiple values and keep callback failures visible.

There is no process-global breaker, limiter, timer, or worker thread.
Clocks, random sources, sleepers, cancellation tokens, event handlers, state
stores, and lease stores are injected at the operation boundary. Local and
distributed circuit-breaker events use the same injected monotonic clock as
their state transitions.

## Documentation

- [Getting started](docs/src/getting-started.md)
- [Core concepts](docs/src/guide/core-concepts.md)
- [Dataflow integration](docs/src/guide/dataflow.md)
- [Recipes](docs/src/guide/recipes.md)
- [API reference](docs/src/reference/api.md)
- [Architecture](docs/src/reference/architecture.md)
- [Runtime contracts](docs/src/reference/runtime-contracts.md)
- [Development](docs/src/project/development.md)

The reference covers cooperative timeout semantics, retry safety, composition
order, process-local versus distributed state, and the event model.

## Development

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

For a direct test run:

```sh
sbcl --non-interactive --no-userinit --no-sysinit \
  --load run-tests.lisp
```

The bootstrap script accepts either adjacent nerima-lisp checkouts or the
shared ghq bare-clone layout and materializes missing sibling sources for the
direct run. The test runner rejects an empty selection. On macOS/aarch64
with SBCL 2.6.0, this direct run currently hangs before any project code
loads, and there is no known workaround; `nix flake check` is the supported
path. See [Development](docs/src/project/development.md) for coverage and
documentation workflow details.
`nix flake check` also runs the structural `paredit` lint check from the
pinned Nerima Lisp package.

The test suite uses cl-weave property and fuzz tests, polling assertions,
continuation-value assertions, mocked boundaries, and
explicit assertion-count contracts. The direct runner applies a 30-second
per-test timeout; the Nix test derivation retains a 120-second process-level
timeout for build and test startup overhead.

## Contributing

Keep resilience policy decisions explicit, preserve injectable boundaries, and
add focused tests for new behavior. Run the narrowest relevant check before
the full Nix check.

## Support

Open an issue in the [GitHub repository](https://github.com/nerima-lisp/cl-resilience-kit)
with a minimal reproduction and the command that exposed the problem.

## License

Released under the MIT license.

# Getting started

## Installation

### ASDF

Make the repository available to ASDF's source registry, then load the system:

```common-lisp
(asdf:load-system :cl-resilience-kit)
```

The production system uses the Nerima Lisp packages `cl-boundary-kit` for
injectable effects, `cl-concurrent-kit` for synchronization, and `cl-date-kit`
for executor timing. The optional `cl-resilience-kit/observability` system
adds direct `cl-observability-kit` metrics, and the optional
`cl-resilience-kit/dataflow` system wraps `cl-dataflow` pipeline handlers with
the same resilience contracts; the test system alone uses `cl-weave`.

Load the ASDF systems by their `cl-...` names, then use the shorter Nerima Lisp
package nicknames in code: `resilience-kit`,
`resilience-observability`, and `resilience-dataflow`.

### Nix

Pin the flake in a consuming project:

```nix
{
  inputs.cl-resilience-kit.url = "github:nerima-lisp/cl-resilience-kit";
}
```

The repository's development shell and checks are described in
[Development](project/development.md). The flake exposes the library package,
tests, coverage, and documentation outputs through the same `cl-nix-forge`
workflow used by the other nerima-lisp libraries.

## First retry

This small example retries a known temporary condition and returns `:ok` on
the third attempt:

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
           (declare (ignore condition attempt))
           t))))
  (call-with-retry
   policy
   (lambda ()
     (incf attempts)
     (if (< attempts 3)
         (error 'temporary-storage-error)
         :ok))))
;; => :OK
```

`max-attempts` includes the initial call. In production, replace the broad
classifier in the example with an application-specific classification of
transient failures. Retry safety is opt-in: a classifier is consulted for
conditions and results only when `:retry-safe-p` is true.

## Choose the boundaries explicitly

Use injected boundaries when reproducibility or integration control matters:

- `:clock` and `:monotonic-units-per-second` for monotonic time;
- `:random-source` for jitter;
- `:sleeper` for backoff waits;
- `:cancellation-token` for cooperative cancellation;
- `:event-handler` or a `resilience-observer` for telemetry;
- a state store and optional lease store for distributed coordination.

See [Recipes](guide/recipes.md) for composed controls and
[Runtime contracts](reference/runtime-contracts.md) for the process and timeout
boundaries.

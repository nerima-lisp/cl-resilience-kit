# Dataflow integration

The core system keeps execution composition independent from any pipeline
library. When an application already uses the Nerima Lisp `cl-dataflow`
package, load the optional ASDF system to keep that API surface while adding
the same resilience policies used by the rest of the library:

```lisp
(asdf:load-system "cl-resilience-kit/dataflow")
(in-package #:resilience-dataflow)
```

The optional system is intended to track the verified Nerima Lisp
`cl-dataflow` release line. In this repository, that means `v1.1.1`.

Load the ASDF system by its `cl-...` name, then use the Nerima Lisp package
names in code: `resilience-kit` for the core API and `resilience-dataflow`
for the integration layer. `cl-resilience-kit` and `cl-resilience-kit/dataflow`
remain available as compatibility nicknames. `resilience-dataflow`
re-exports the direct `cl-dataflow` entry points (`define-pipeline`,
`make-node`, `make-pipeline`, `pipeline`, `pipeline->node`, and
`run-pipeline`) alongside the resilience-specific constructors.

Build a resilience-aware pipeline with the same declarative shape as
`cl-dataflow:define-pipeline`. Each `:node` accepts the normal `:handler`, plus
an explicit `:resilience-options` plist consumed by `cl-resilience-kit`:

```lisp
(let* ((policy
         (resilience-kit:make-retry-policy
          :max-attempts 3
          :initial-delay 0.05d0
          :retry-safe-p t
          :condition-classifier
          (lambda (condition attempt)
            (declare (ignore attempt))
            (typep condition 'temporary-storage-error))))
       (pipeline
         (define-resilience-pipeline ()
           (:node "fetch-profile"
            :handler
            (lambda (input context)
              (declare (ignore context))
              (fetch-profile input))
            :resilience-options
            (list :operation :fetch-profile
                  :retry-policy policy)))))
  (run-resilience-pipeline pipeline :input 42))
```

`define-resilience-pipeline` is the preferred entry point when the pipeline is
known at source level: the declarative graph stays in one macro form, while
the runtime constructors remain available for generated or embedded nodes.
`make-resilience-node`, `make-resilience-pipeline`, and
`make-resilience-pipeline-node` cover that programmatic layer. In every case,
the `:resilience-options` plist is forwarded directly to
`resilience-kit:call-with-resilience`, so retry, breaker, limiter, and
observer configuration stays in one place.

At macro-expansion time, `define-resilience-pipeline` consumes only the extra
`cl-resilience-kit` keys `:handler` and `:resilience-options` from each
`cl-dataflow` `:node` clause, rewrites the
handler once, and forwards the rest of the node shape unchanged to
`cl-dataflow`. That keeps the declarative pipeline form readable while leaving
node metadata, inputs, outputs, and other `cl-dataflow` options in their
original place.

The integration is optional: applications that do not need `cl-dataflow` can
continue to depend on the base `cl-resilience-kit` system alone.

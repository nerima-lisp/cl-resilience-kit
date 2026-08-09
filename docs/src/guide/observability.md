# Observability integration

The core system keeps its metrics implementation dependency-neutral. When an
application already uses `cl-observability-kit`, load the optional ASDF system
to publish resilience events as metrics:

```lisp
(asdf:load-system "cl-resilience-kit/observability")
```

Create one observability object for the event stream you want to measure. The
integration defines an event counter and a duration histogram in the supplied
`cl-observability-kit` registry. Both metrics use the stable `event_type` and
`operation` labels.

```lisp
(let ((observability
        (cl-resilience-kit/observability:make-resilience-observability)))
  (cl-resilience-kit/observability:with-resilience-observability
      (observability)
    (cl-resilience-kit:call-with-resilience
     (lambda ()
       (perform-operation))
     :operation :checkout
     :retry-policy retry-policy)))
```

`with-resilience-observability` installs the handler through the dynamic
resilience context, so composed controls inherit it without repeating an
`:event-handler` keyword. An explicit handler passed to
`call-with-resilience` still takes precedence; use an explicit handler when
you need to combine application-specific processing with metrics.

The event counter records every resilience event. The duration histogram only
records finite, non-negative event durations. This keeps invalid or absent
duration values out of the backend while preserving the original event for
other handlers.

The integration is optional: applications that do not need
`cl-observability-kit` can continue to depend on the base
`cl-resilience-kit` system alone.

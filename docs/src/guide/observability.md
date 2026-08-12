# Observability integration

The core system keeps its metrics implementation dependency-neutral. When an
application already uses `cl-observability-kit`, load the optional ASDF system
to publish resilience events as metrics:

```lisp
(asdf:load-system "cl-resilience-kit/observability")
(in-package #:resilience-observability)
```

Load the ASDF system by its `cl-...` name, then use the shorter Nerima Lisp
package nicknames in code: `resilience-kit` for the core API and
`resilience-observability` for the integration layer.

Create one observability object for the event stream you want to measure. The
integration defines an event counter and a duration histogram in the supplied
`cl-observability-kit` registry. Both metrics use the stable `event_type` and
`operation` labels.

```lisp
(let ((observability
        (make-resilience-observability)))
  (with-resilience-observability
      (observability)
    (resilience-kit:call-with-resilience
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

When the dynamic wrapper does not fit the call site, use the direct handler or
record API explicitly. This keeps the Nerima Lisp package boundary visible in
the integration code instead of threading ad hoc metric updates through the
application:

```lisp
(let* ((observability
         (make-resilience-observability))
       (handler
         (resilience-observability-handler
          observability)))
  (resilience-kit:call-with-resilience
   (lambda ()
     (perform-operation))
   :operation :checkout
   :event-handler handler))
```

If the event already exists, publish it directly:

```lisp
(record-resilience-event
 observability
 (resilience-kit:make-resilience-event
  :type :operation-complete
  :operation :checkout
  :duration 0.12d0))
```

The event counter records every resilience event. The duration histogram only
records finite, non-negative event durations. This keeps invalid or absent
duration values out of the backend while preserving the original event for
other handlers.

The integration is optional: applications that do not need
`cl-observability-kit` can continue to depend on the base
`cl-resilience-kit` system alone.

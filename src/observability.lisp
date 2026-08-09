(defpackage #:cl-resilience-kit/observability
  (:nicknames #:resilience-observability)
  (:use #:cl)
  (:import-from #:cl-resilience-kit
                #:resilience-event
                #:resilience-event-duration
                #:resilience-event-operation
                #:resilience-event-type
                #:with-resilience-event-handler)
  (:import-from #:observability-kit
                #:define-counter
                #:define-histogram
                #:make-metric-registry
                #:metric-inc
                #:metric-observe
                #:metric-registry)
  (:export #:resilience-observability
           #:resilience-observability-p
           #:make-resilience-observability
           #:resilience-observability-registry
           #:resilience-observability-events
           #:resilience-observability-durations
           #:resilience-observability-handler
           #:record-resilience-event
           #:with-resilience-observability))

(in-package #:cl-resilience-kit/observability)

(defstruct (resilience-observability
            (:constructor %make-resilience-observability
                (registry events durations)))
  "Metrics published for one resilience event stream."
  registry
  events
  durations)

(defun make-resilience-observability
    (&key registry (cardinality-limit 256))
  "Create an observability-kit registry for resilience event metrics."
  (when registry
    (check-type registry metric-registry))
  (let* ((active-registry
           (or registry
               (make-metric-registry
                :default-cardinality-limit cardinality-limit)))
         (events
           (define-counter
            active-registry resilience_events_total
            :help "Number of resilience events."
            :unit "events"
            :label-names '("event_type" "operation")
            :cardinality-limit cardinality-limit))
         (durations
           (define-histogram
            active-registry resilience_event_duration_seconds
            :help "Observed resilience event duration."
            :unit "seconds"
            :label-names '("event_type" "operation")
            :cardinality-limit cardinality-limit)))
    (%make-resilience-observability active-registry events durations)))

(defun %event-label (value)
  (typecase value
    (null "unknown")
    (string (if (plusp (length value)) value "unknown"))
    (symbol (string-downcase (symbol-name value)))
    (t (princ-to-string value))))

(defun %event-labels (event)
  (list (cons "event_type"
              (%event-label (resilience-event-type event)))
        (cons "operation"
              (%event-label (resilience-event-operation event)))))

(defun %finite-duration-seconds (duration)
  (when (and (realp duration) (not (minusp duration)))
    (handler-case
        (let ((seconds (float duration 1d0)))
          (when (and (= seconds seconds)
                     (<= seconds most-positive-double-float))
            seconds))
      (error () nil))))

(defun record-resilience-event (observability event)
  "Publish EVENT to OBSERVABILITY and return EVENT."
  (check-type observability resilience-observability)
  (check-type event resilience-event)
  (let ((labels (%event-labels event)))
    (metric-inc (resilience-observability-events observability)
                1
                :labels labels)
    (let ((duration
            (%finite-duration-seconds
             (resilience-event-duration event))))
      (when duration
        (metric-observe (resilience-observability-durations observability)
                        duration
                        :labels labels))))
  event)

(defun resilience-observability-handler (observability)
  "Return an event handler that records events in OBSERVABILITY."
  (check-type observability resilience-observability)
  (lambda (event)
    (record-resilience-event observability event)))

(defmacro with-resilience-observability ((observability) &body body)
  "Bind OBSERVABILITY's metrics handler around BODY."
  `(with-resilience-event-handler
       ((resilience-observability-handler ,observability))
     ,@body))

(in-package #:resilience-kit)

(defstruct (resilience-event
            (:constructor make-resilience-event
                (&key type operation attempt stage condition result delay
                      reason timestamp context metadata duration)))
  "A best-effort observation emitted by a resilience control.

TYPE and STAGE are implementation-defined keywords.  CONDITION and RESULT
hold the relevant operation values, while TIMESTAMP is monotonic seconds
when the event was created."
  type
  operation
  attempt
  stage
  condition
  result
  delay
  reason
  timestamp
  context
  metadata
  duration)


(defun %active-event-handler (handler)
  (or handler *resilience-event-handler*))

(defun %warn-observer-error (phase handler event cause)
  (warn 'resilience-observer-warning
        :phase phase
        :handler handler
        :event event
        :cause cause))

(defun %call-event-handler (handler event phase)
  (handler-case
      (funcall handler event)
    (error (cause)
      (%warn-observer-error phase handler event cause)
      nil)))

(defun %emit-resilience-event
    (handler type &key operation attempt stage condition result delay reason
                       timestamp context metadata duration clock
                       monotonic-units-per-second)
  "Invoke HANDLER with one RESILIENCE-EVENT, warning on handler errors."
  (let ((active-handler (%active-event-handler handler)))
    (when active-handler
      (let ((event
              (make-resilience-event
               :type type
               :operation operation
               :attempt attempt
               :stage stage
               :condition condition
               :result result
               :delay delay
               :reason reason
               :context (or context *resilience-context*)
               :metadata metadata
               :duration duration
               :timestamp (or timestamp
                              (%now :clock clock
                                    :monotonic-units-per-second
                                    monotonic-units-per-second)))))
        (%call-event-handler active-handler event :event-handler)))
    nil))


(defclass resilience-metrics ()
  ((%lock
    :initarg :lock
    :reader %resilience-metrics-lock)
   (%counts
    :initform (make-hash-table :test #'equal)
    :reader %resilience-metrics-counts)
   (%durations
    :initform (make-hash-table :test #'equal)
    :reader %resilience-metrics-durations)
   (%total-events
    :initform 0
    :accessor %resilience-metrics-total-events)))

(defun make-resilience-metrics (&key name)
  (make-instance
   'resilience-metrics
   :lock (make-lock
          :name (or name "cl-resilience-kit.metrics"))))

(defun %resilience-metrics-key (type operation)
  (list type operation))

(defun record-resilience-event (metrics event)
  "Record EVENT in METRICS and return EVENT.

Metrics are intentionally low-cardinality only when callers use stable
operation names.  No labels are synthesized from condition text."
  (check-type metrics resilience-metrics)
  (check-type event resilience-event)
  (let ((key (%resilience-metrics-key
              (resilience-event-type event)
              (resilience-event-operation event))))
    (with-lock-held ((%resilience-metrics-lock metrics))
      (incf (%resilience-metrics-total-events metrics))
      (incf (gethash key (%resilience-metrics-counts metrics) 0))
      (let ((duration (%finite-duration-seconds
                       (resilience-event-duration event))))
        (when duration
          (incf (gethash key (%resilience-metrics-durations metrics) 0d0)
                duration)))))
  event)

(defun resilience-metrics-total-events (metrics)
  (check-type metrics resilience-metrics)
  (with-lock-held ((%resilience-metrics-lock metrics))
    (%resilience-metrics-total-events metrics)))

(defun resilience-metrics-count (metrics type &key operation)
  (check-type metrics resilience-metrics)
  (with-lock-held ((%resilience-metrics-lock metrics))
    (gethash (%resilience-metrics-key type operation)
             (%resilience-metrics-counts metrics)
             0)))

(defun resilience-metrics-duration (metrics type &key operation)
  (check-type metrics resilience-metrics)
  (with-lock-held ((%resilience-metrics-lock metrics))
    (gethash (%resilience-metrics-key type operation)
             (%resilience-metrics-durations metrics)
             0d0)))

(defun resilience-metrics-snapshot (metrics)
  "Return an immutable-by-convention snapshot as a property list."
  (check-type metrics resilience-metrics)
  (with-lock-held ((%resilience-metrics-lock metrics))
    (list
     :total-events (%resilience-metrics-total-events metrics)
     :events (loop for key being the hash-keys of (%resilience-metrics-counts metrics)
                   using (hash-value value)
                   collect (cons (copy-tree key) value))
     :durations (loop for key being the hash-keys of (%resilience-metrics-durations metrics)
                      using (hash-value value)
                      collect (cons (copy-tree key) value)))))

(defun reset-resilience-metrics (metrics)
  (check-type metrics resilience-metrics)
  (with-lock-held ((%resilience-metrics-lock metrics))
    (clrhash (%resilience-metrics-counts metrics))
    (clrhash (%resilience-metrics-durations metrics))
    (setf (%resilience-metrics-total-events metrics) 0))
  metrics)

(defun resilience-metrics-handler (metrics)
  "Return an event handler that records events in METRICS."
  (check-type metrics resilience-metrics)
  (lambda (event)
    (record-resilience-event metrics event)))

(defclass resilience-observer ()
  ((handlers
    :initarg :handlers
    :reader resilience-observer-handlers)))

(defun make-resilience-observer (&rest handlers)
  (dolist (handler handlers)
    (check-type handler function))
  (make-instance 'resilience-observer :handlers (copy-list handlers)))

(defun resilience-observer-handler (observer)
  "Return a fan-out handler.  One failing observer warns and does not silence others."
  (check-type observer resilience-observer)
  (lambda (event)
    (dolist (handler (resilience-observer-handlers observer))
      (%call-event-handler handler event :observer))
    event))

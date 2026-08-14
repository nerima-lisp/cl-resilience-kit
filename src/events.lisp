(in-package #:resilience-kit)

(defstruct (resilience-event
            (:constructor %make-resilience-event
                (type operation attempt stage condition result delay reason
                      timestamp context metadata duration))
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

(defun %emit-resilience-event*
    (handler type operation attempt stage condition result delay reason
     timestamp context metadata duration clock monotonic-units-per-second)
  (let ((active-handler (%active-event-handler handler))
        (metrics *resilience-event-metrics*))
    (unless (or active-handler metrics)
      (return-from %emit-resilience-event* nil))
    (when active-handler
      (let ((event
              (%make-resilience-event
               type
               operation
               attempt
               stage
               condition
               result
               delay
               reason
               (or timestamp
                   (%now :clock clock
                         :monotonic-units-per-second
                         monotonic-units-per-second))
               (or context *resilience-context*)
               metadata
               duration)))
      (handler-case
          (funcall active-handler event)
        (error () nil))))
    (when metrics
      (%record-resilience-event* metrics
                                 type
                                 operation
                                 duration)))
  nil)

(defun %emit-resilience-event
    (handler type &key operation attempt stage condition result delay reason
                       timestamp context metadata duration clock
                       monotonic-units-per-second)
  "Invoke HANDLER with one RESILIENCE-EVENT, swallowing handler errors."
  (%emit-resilience-event* handler
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
                           duration
                           clock
                           monotonic-units-per-second))

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
   (%handler :initarg :handler
             :type function
             :reader %resilience-metrics-handler-function)
   (%total-events
    :initform 0
    :type fixnum
    :accessor %resilience-metrics-total-events)))

(defun make-resilience-metrics (&key name)
  (let ((metrics
          (make-instance
           'resilience-metrics
           :lock (cl-concurrent-kit:make-lock
                  :name (or name "cl-resilience-kit.metrics")))))
    (setf (slot-value metrics '%handler)
          (lambda (event)
            (%record-resilience-event metrics event)))
    metrics))

(defun %resilience-metrics-key (type operation)
  (list type operation))

(defun %record-resilience-event (metrics event)
  (declare (type resilience-metrics metrics)
           (type resilience-event event))
  (let ((duration (resilience-event-duration event)))
    (%record-resilience-event* metrics
                               (resilience-event-type event)
                               (resilience-event-operation event)
                               duration))
  event)

(defun %record-resilience-event* (metrics type operation duration)
  (declare (type resilience-metrics metrics))
  (let ((key (%resilience-metrics-key type operation)))
    (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
      (incf (%resilience-metrics-total-events metrics))
      (incf (gethash key (%resilience-metrics-counts metrics) 0))
      (when duration
        (incf (gethash key (%resilience-metrics-durations metrics) 0d0)
              (float duration 1d0)))))
  metrics)

(defun record-resilience-event (metrics event)
  "Record EVENT in METRICS and return EVENT.

  Metrics are intentionally low-cardinality only when callers use stable
operation names.  No labels are synthesized from condition text."
  (check-type metrics resilience-metrics)
  (check-type event resilience-event)
  (%record-resilience-event metrics event))

(defun resilience-metrics-total-events (metrics)
  (check-type metrics resilience-metrics)
  (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
    (%resilience-metrics-total-events metrics)))

(defun resilience-metrics-count (metrics type &key operation)
  (check-type metrics resilience-metrics)
  (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
    (gethash (%resilience-metrics-key type operation)
             (%resilience-metrics-counts metrics)
             0)))

(defun resilience-metrics-duration (metrics type &key operation)
  (check-type metrics resilience-metrics)
  (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
    (gethash (%resilience-metrics-key type operation)
             (%resilience-metrics-durations metrics)
             0d0)))

(defun resilience-metrics-snapshot (metrics)
  "Return an immutable-by-convention snapshot as a property list."
  (check-type metrics resilience-metrics)
  (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
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
  (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
    (clrhash (%resilience-metrics-counts metrics))
    (clrhash (%resilience-metrics-durations metrics))
    (setf (%resilience-metrics-total-events metrics) 0))
  metrics)

(defun resilience-metrics-handler (metrics)
  "Return an event handler that records events in METRICS."
  (check-type metrics resilience-metrics)
  (%resilience-metrics-handler-function metrics))

(defun %make-resilience-metrics-handler (metrics)
  (resilience-metrics-handler metrics))

(defclass resilience-observer ()
  ((handlers
    :initarg :handlers
    :type list
    :reader resilience-observer-handlers)
   (%handler
    :initarg :handler
    :type function
    :reader %resilience-observer-handler-function)))

(defun make-resilience-observer (&rest handlers)
  (dolist (handler handlers)
    (check-type handler function))
  (apply #'%make-resilience-observer handlers))

(defun resilience-observer-p (object)
  (typep object 'resilience-observer))

(defun %resilience-observer-handlers (observer)
  (check-type observer resilience-observer)
  (slot-value observer 'handlers))

(defun %call-resilience-observer-handlers (handlers event)
  (dolist (handler handlers)
    (handler-case
        (funcall handler event)
      (error () nil)))
  event)

(defun %call-resilience-observer-handler (handler event)
  (handler-case
      (funcall handler event)
    (error () nil))
  event)

(defun %call-two-resilience-observer-handlers
    (first-handler second-handler event)
  (handler-case
      (funcall first-handler event)
    (error () nil))
  (handler-case
      (funcall second-handler event)
    (error () nil))
  event)

(defun %make-resilience-observer-handler (handlers)
  (cond
    ((null handlers)
     (lambda (event)
       event))
    ((null (cdr handlers))
     (let ((handler (car handlers)))
       (lambda (event)
         (%call-resilience-observer-handler handler event))))
    ((null (cddr handlers))
     (let ((first-handler (car handlers))
           (second-handler (cadr handlers)))
       (lambda (event)
         (%call-two-resilience-observer-handlers
          first-handler second-handler event))))
    (t
     (lambda (event)
       (%call-resilience-observer-handlers handlers event)))))

(defun resilience-observer-handler (observer)
  "Return a fan-out handler.  One failing observer does not silence others."
  (check-type observer resilience-observer)
  (%resilience-observer-handler-function observer))

(defun %make-resilience-observer (&rest handlers)
  (dolist (handler handlers)
    (check-type handler function))
  (let ((observer-handlers (copy-list handlers)))
    (make-instance 'resilience-observer
                   :handlers observer-handlers
                   :handler (%make-resilience-observer-handler
                             observer-handlers))))

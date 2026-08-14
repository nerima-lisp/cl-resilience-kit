(defpackage #:resilience-observability
  (:nicknames #:cl-resilience-kit/observability)
  (:use #:cl)
  (:import-from #:resilience-kit
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

(in-package #:resilience-observability)

(defstruct (resilience-observability
            (:constructor %make-resilience-observability
                (registry events durations %handler %label-cache
                 %default-duration-buckets-p %state-cache)))
  "Metrics published for one resilience event stream."
  registry
  events
  durations
  (%label-cache (make-hash-table :test #'equal) :read-only t)
  (%default-duration-buckets-p nil :read-only t)
  (%state-cache (vector nil nil nil) :read-only t)
  (%handler nil :read-only t))

(defstruct (%observability-label-state
            (:constructor %make-observability-label-state
                (labels events-series durations-series)))
  labels
  events-series
  durations-series)

(declaim (inline %event-label
                 %ensure-observability-label-bucket
                 %event-state
                 %ensure-observability-metric-series
                 %observability-cache-state
                 %observability-series-inc
                 %record-observability-event/default*
                 %record-observability-event/custom*
                 %observability-default-duration-buckets-p
                 %observability-increment-default-buckets
                 %observability-increment-buckets
                 %finite-duration-seconds
                 %record-observability-event*))

(defun %record-observability-event/default*
    (label-cache events durations state-cache event)
  (let ((state (%event-state label-cache events durations state-cache event)))
    (%observability-series-inc
     (%observability-label-state-events-series state))
    (let ((duration
            (%finite-duration-seconds
             (resilience-event-duration event))))
      (declare (type (or null double-float) duration))
      (when duration
        (let ((series
                (%observability-label-state-durations-series state)))
          (incf (observability-kit::%metric-series-count series))
          (incf (observability-kit::%metric-series-sum series) duration)
          (%observability-increment-default-buckets series duration)))))
  event)

(defun %record-observability-event/custom*
    (label-cache events durations state-cache event)
  (let ((state (%event-state label-cache events durations state-cache event)))
    (%observability-series-inc
     (%observability-label-state-events-series state))
    (let ((duration
            (%finite-duration-seconds
             (resilience-event-duration event))))
      (declare (type (or null double-float) duration))
      (when duration
        (let ((series
                (%observability-label-state-durations-series state)))
          (incf (observability-kit::%metric-series-count series))
          (incf (observability-kit::%metric-series-sum series) duration)
          (%observability-increment-buckets durations series duration)))))
  event)

(defun %record-observability-event*
    (label-cache events durations default-duration-buckets-p state-cache event)
  (if default-duration-buckets-p
      (%record-observability-event/default*
       label-cache events durations state-cache event)
      (%record-observability-event/custom*
       label-cache events durations state-cache event)))

(defun make-resilience-observability
    (&key registry (cardinality-limit 256))
  "Create an observability-kit registry for resilience event metrics."
  (when registry
    (check-type registry metric-registry))
  (let* ((active-registry
           (or registry
               (make-metric-registry
                :default-cardinality-limit cardinality-limit)))
         (label-cache
           (make-hash-table :test #'equal))
         (events
           (define-counter
            active-registry 'resilience_events_total
            :help "Number of resilience events."
            :unit "events"
            :label-names '("event_type" "operation")
            :cardinality-limit cardinality-limit))
         (durations
           (define-histogram
            active-registry 'resilience_event_duration_seconds
            :help "Observed resilience event duration."
            :unit "seconds"
            :label-names '("event_type" "operation")
            :cardinality-limit cardinality-limit))
         (default-duration-buckets-p
           (%observability-default-duration-buckets-p durations))
         (state-cache
           (vector nil nil nil)))
    (%make-resilience-observability
     active-registry
     events
     durations
     (if default-duration-buckets-p
         (lambda (event)
           (%record-observability-event/default*
            label-cache events durations state-cache event))
         (lambda (event)
           (%record-observability-event/custom*
            label-cache events durations state-cache event)))
     label-cache
     default-duration-buckets-p
     state-cache)))

(defun %event-label (value)
  (typecase value
    (null "unknown")
    (string (if (plusp (length value)) value "unknown"))
    (symbol (string-downcase (symbol-name value)))
    (t (princ-to-string value))))

(defun %ensure-observability-label-bucket (cache event-type)
  (or (gethash event-type cache)
      (setf (gethash event-type cache)
            (make-hash-table :test #'equal))))

(defun %observability-cache-state (state-cache event-type operation)
  (let ((cached-event-type (svref state-cache 0))
        (cached-operation (svref state-cache 1)))
    (when (and (or (eq event-type cached-event-type)
                   (equal event-type cached-event-type))
               (or (eq operation cached-operation)
                   (equal operation cached-operation)))
      (svref state-cache 2))))

(defun %event-state (cache events durations state-cache event)
  (let* ((event-type (resilience-event-type event))
         (operation (resilience-event-operation event))
         (cached-state
           (%observability-cache-state state-cache event-type operation)))
    (or cached-state
        (let* ((bucket (%ensure-observability-label-bucket cache event-type))
               (state
                 (or (gethash operation bucket)
                     (let ((labels
                             (list (cons "event_type"
                                         (%event-label event-type))
                                   (cons "operation"
                                         (%event-label operation)))))
                       (setf (gethash operation bucket)
                             (%make-observability-label-state
                              labels
                              (%ensure-observability-metric-series events labels)
                              (%ensure-observability-metric-series durations labels)))))))
          (setf (svref state-cache 0) event-type
                (svref state-cache 1) operation
                (svref state-cache 2) state)
          state))))

(defun %ensure-observability-metric-series (metric labels)
  (cl-concurrent-kit:with-lock-held
      ((observability-kit::%metric-lock metric))
    (or (gethash labels
                 (observability-kit::%metric-series metric))
        (let ((metric-series
                (observability-kit::%metric-series metric)))
          (when (>= (hash-table-count metric-series)
                    (observability-kit::%metric-cardinality-limit metric))
            (error 'observability-kit::metric-cardinality-exceeded
                   :name (observability-kit::%metric-name metric)
                   :limit (observability-kit::%metric-cardinality-limit metric)
                   :labels labels
                   :message (format nil
                                    "Metric ~S exceeded its cardinality limit of ~D."
                                    (observability-kit::%metric-name metric)
                                    (observability-kit::%metric-cardinality-limit
                                     metric))))
          (let ((series
                  (observability-kit::%empty-series
                   (observability-kit::%metric-kind metric)
                   (observability-kit::%metric-histogram-buckets metric)
                   labels)))
            (setf (gethash labels metric-series) series)
            (setf (observability-kit::%metric-series-order metric)
                  (observability-kit::%insert-metric-series
                   (observability-kit::%metric-series-order metric)
                   series))
            series)))))

(defun %observability-series-inc (series)
  (incf (observability-kit::%metric-series-value series))
  (observability-kit::%metric-series-value series))

(defun %observability-default-duration-buckets-p (metric)
  (equal (observability-kit::%metric-histogram-buckets metric)
         '(0.001d0 0.005d0 0.01d0 0.05d0 0.1d0 0.5d0 1d0)))

(defun %observability-increment-default-buckets (series observation)
  (let* ((counts (observability-kit::%metric-series-bucket-counts series))
         (c1 counts)
         (c2 (cdr c1))
         (c3 (cdr c2))
         (c4 (cdr c3))
         (c5 (cdr c4))
         (c6 (cdr c5))
         (c7 (cdr c6))
         (c8 (cdr c7)))
    (declare (cons c1 c2 c3 c4 c5 c6 c7 c8)
             (double-float observation))
    (when (<= observation 0.001d0)
      (incf (car c1)))
    (when (<= observation 0.005d0)
      (incf (car c2)))
    (when (<= observation 0.01d0)
      (incf (car c3)))
    (when (<= observation 0.05d0)
      (incf (car c4)))
    (when (<= observation 0.1d0)
      (incf (car c5)))
    (when (<= observation 0.5d0)
      (incf (car c6)))
    (when (<= observation 1d0)
      (incf (car c7)))
    (incf (car c8))))

(defun %observability-increment-buckets (metric series observation)
  (let ((count-cell (observability-kit::%metric-series-bucket-counts series)))
    (dolist (bucket (observability-kit::%metric-histogram-buckets metric))
      (when (<= observation bucket)
        (incf (car count-cell)))
      (setf count-cell (cdr count-cell)))
    (incf (car count-cell))))

(defun %finite-duration-seconds (duration)
  (typecase duration
    (null nil)
    (double-float
      (and (not (minusp duration))
           (= duration duration)
           (<= duration most-positive-double-float)
           duration))
    (single-float
      (and (not (minusp duration))
           (let ((seconds (float duration 1d0)))
             (and (= seconds seconds)
                  (<= seconds most-positive-double-float)
                  seconds))))
    (rational
      (and (not (minusp duration))
           (<= duration most-positive-double-float)
           (float duration 1d0)))
    (t nil)))

(defun record-resilience-event (observability event)
  "Publish EVENT to OBSERVABILITY and return EVENT."
  (check-type observability resilience-observability)
  (check-type event resilience-event)
  (%record-observability-event*
   (resilience-observability-%label-cache observability)
   (resilience-observability-events observability)
   (resilience-observability-durations observability)
   (resilience-observability-%default-duration-buckets-p observability)
   (resilience-observability-%state-cache observability)
   event))

(defun resilience-observability-handler (observability)
  "Return an event handler that records events in OBSERVABILITY."
  (check-type observability resilience-observability)
  (resilience-observability-%handler observability))

(defmacro with-resilience-observability ((observability) &body body)
  "Bind OBSERVABILITY's metrics handler around BODY."
  `(with-resilience-event-handler
       ((resilience-observability-handler ,observability))
     ,@body))

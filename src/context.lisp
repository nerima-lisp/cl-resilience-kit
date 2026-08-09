(in-package #:cl-resilience-kit)

(defparameter +default-monotonic-units-per-second+
  (coerce internal-time-units-per-second 'double-float))

(defvar *resilience-clock* nil)
(defvar *resilience-monotonic-units-per-second* nil)
(defvar *resilience-deadline* nil)
(defvar *resilience-cancellation-token* nil)
(defvar *resilience-event-handler* nil)
(defvar *resilience-context* nil)


(defun %finite-real-p (value)
  "Return true when VALUE is a finite real representable as a double float."
  (and (realp value)
       (= value value)
       (handler-case
           (let ((double (float value 1d0)))
             (and (= double double)
                  (<= (abs double) most-positive-double-float)))
         (error () nil))))

(defun %ensure-non-negative-real (value name)
  (unless (and (%finite-real-p value) (not (minusp value)))
    (error "~A must be a non-negative real, got ~S." name value))
  (float value 1d0))

(defun %ensure-positive-real (value name)
  (unless (and (%finite-real-p value) (plusp value))
    (error "~A must be a positive real, got ~S." name value))
  (float value 1d0))

(defun %active-clock (clock)
  (or clock *resilience-clock* (cl-boundary-kit:make-clock)))

(defun %active-monotonic-units-per-second (units)
  (%ensure-positive-real
   (or units
       *resilience-monotonic-units-per-second*
       +default-monotonic-units-per-second+)
   "MONOTONIC-UNITS-PER-SECOND"))

(defun %monotonic-seconds (clock monotonic-units-per-second)
  (/ (float (cl-boundary-kit:clock-monotonic clock) 1d0)
     (float monotonic-units-per-second 1d0)))

(defun %now (&key clock monotonic-units-per-second)
  (let ((active-clock (%active-clock clock))
        (units (%active-monotonic-units-per-second
                monotonic-units-per-second)))
    (%monotonic-seconds active-clock units)))

(defun %active-sleeper (sleeper)
  (or sleeper (cl-boundary-kit:make-sleeper)))

(defun %sleep (sleeper seconds)
  (cl-boundary-kit:sleeper-sleep sleeper seconds))

(defun %active-random-source (random-source)
  (or random-source (cl-boundary-kit:make-random-source)))

(defun %random-unit (random-source)
  (/ (float (cl-boundary-kit:random-source-random random-source 1000000)
            1d0)
     1000000d0))

(defun %effective-deadline (requested-deadline)
  (if (and requested-deadline *resilience-deadline*)
      (min requested-deadline *resilience-deadline*)
      (or requested-deadline *resilience-deadline*)))

(defun %deadline-remaining-at (now deadline)
  (when deadline
    (max 0d0 (- deadline now))))

(defun %make-deadline-condition
    (clock monotonic-units-per-second deadline &key operation stage attempt)
  (make-condition
   'deadline-exceeded
   :message (format nil "The ~A deadline was exceeded." (or stage :operation))
   :operation operation
   :deadline deadline
   :observed-at (%monotonic-seconds clock monotonic-units-per-second)
   :stage (or stage :operation)
   :attempt attempt))

(defun %signal-deadline-exceeded
    (clock monotonic-units-per-second deadline &key operation stage attempt)
  (error (%make-deadline-condition clock monotonic-units-per-second deadline
                                   :operation operation
                                   :stage stage
                                   :attempt attempt)))

;;; Request context and observation

(defstruct (resilience-context
            (:constructor make-resilience-context
                (&key operation correlation-id trace-id span-id parent-span-id
                      idempotency-key tags baggage)))
  "Portable correlation and propagation data for one resilience operation.

The structure deliberately contains no transport-specific tracing type.  An
integration may copy these values into HTTP headers, message metadata, or a
tracing SDK without making the core depend on that transport."
  operation
  correlation-id
  trace-id
  span-id
  parent-span-id
  idempotency-key
  tags
  baggage)

(defun current-resilience-context ()
  *resilience-context*)

(defun merge-resilience-context (base overlay)
  "Return OVERLAY merged on top of BASE, preserving omitted parent values."
  (check-type overlay resilience-context)
  (if (null base)
      overlay
      (progn
        (check-type base resilience-context)
        (make-resilience-context
         :operation (or (resilience-context-operation overlay)
                        (resilience-context-operation base))
         :correlation-id (or (resilience-context-correlation-id overlay)
                             (resilience-context-correlation-id base))
         :trace-id (or (resilience-context-trace-id overlay)
                       (resilience-context-trace-id base))
         :span-id (or (resilience-context-span-id overlay)
                      (resilience-context-span-id base))
         :parent-span-id (or (resilience-context-parent-span-id overlay)
                             (resilience-context-parent-span-id base))
         :idempotency-key (or (resilience-context-idempotency-key overlay)
                             (resilience-context-idempotency-key base))
         :tags (append (copy-tree (resilience-context-tags overlay))
                       (copy-tree (resilience-context-tags base)))
         :baggage (append (copy-tree (resilience-context-baggage overlay))
                          (copy-tree (resilience-context-baggage base)))))))

(defmacro with-resilience-context ((&rest initargs) &body body)
  "Bind a merged RESILIENCE-CONTEXT for BODY."
  `(let ((*resilience-context*
           (merge-resilience-context
            *resilience-context*
            (make-resilience-context ,@initargs))))
     ,@body))

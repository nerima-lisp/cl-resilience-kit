(in-package #:resilience-kit)

(defparameter +default-monotonic-units-per-second+
  (coerce internal-time-units-per-second 'double-float))

(defparameter +default-clock+
  (load-time-value
   (cl-boundary-kit:make-clock)
   t))

(defparameter +default-sleeper+
  (load-time-value
   (cl-boundary-kit:make-sleeper)
   t))

(defvar *resilience-clock* nil)
(defvar *resilience-monotonic-units-per-second* nil)
(defvar *resilience-deadline* nil)
(defvar *resilience-cancellation-token* nil)
(defvar *resilience-event-handler* nil)
(defvar *resilience-event-metrics* nil)
(defvar *resilience-context* nil)

(declaim (inline %unpack-resilience-values
                 %resilience-primary-value
                 %merge-resilience-context-operation))

(defparameter +no-resilience-values+
  (list :no-resilience-values))

(defstruct (%packed-resilience-values-2
            (:constructor %make-packed-resilience-values-2 (first second)))
  first
  second)

(defstruct (%packed-resilience-values-3
            (:constructor %make-packed-resilience-values-3 (first second third)))
  first
  second
  third)

(defstruct (%packed-resilience-values
            (:constructor %make-packed-resilience-values (values)))
  values)

(defmacro %pack-resilience-values (form)
  `(multiple-value-call
       (lambda (&optional (first +no-resilience-values+ first-p)
                          (second nil second-p)
                          (third nil third-p)
                          &rest rest)
         (cond
           ((not first-p)
            +no-resilience-values+)
           ((not second-p)
            first)
           ((not third-p)
            (%make-packed-resilience-values-2 first second))
           ((null rest)
            (%make-packed-resilience-values-3 first second third))
           (t
            (%make-packed-resilience-values
             (list* first second third rest)))))
     ,form))

(defun %unpack-resilience-values (packed)
  (cond
    ((eq packed +no-resilience-values+)
     (values))
    ((typep packed '%packed-resilience-values-2)
     (values (%packed-resilience-values-2-first packed)
             (%packed-resilience-values-2-second packed)))
    ((typep packed '%packed-resilience-values-3)
     (values (%packed-resilience-values-3-first packed)
             (%packed-resilience-values-3-second packed)
             (%packed-resilience-values-3-third packed)))
    ((typep packed '%packed-resilience-values)
     (values-list (%packed-resilience-values-values packed)))
    (t
     (values packed))))

(defun %resilience-primary-value (packed)
  (cond
    ((eq packed +no-resilience-values+)
     nil)
    ((typep packed '%packed-resilience-values-2)
     (%packed-resilience-values-2-first packed))
    ((typep packed '%packed-resilience-values-3)
     (%packed-resilience-values-3-first packed))
    ((typep packed '%packed-resilience-values)
     (car (%packed-resilience-values-values packed)))
    (t
     packed)))


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
  (or clock *resilience-clock* +default-clock+))

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
  (or sleeper +default-sleeper+))

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
      (let ((checked-base base))
        (check-type checked-base resilience-context)
        (let ((overlay-operation (resilience-context-operation overlay))
              (base-operation (resilience-context-operation checked-base))
              (overlay-correlation-id
                (resilience-context-correlation-id overlay))
              (base-correlation-id
                (resilience-context-correlation-id checked-base))
              (overlay-trace-id (resilience-context-trace-id overlay))
              (base-trace-id (resilience-context-trace-id checked-base))
              (overlay-span-id (resilience-context-span-id overlay))
              (base-span-id (resilience-context-span-id checked-base))
              (overlay-parent-span-id
                (resilience-context-parent-span-id overlay))
              (base-parent-span-id
                (resilience-context-parent-span-id checked-base))
              (overlay-idempotency-key
                (resilience-context-idempotency-key overlay))
              (base-idempotency-key
                (resilience-context-idempotency-key checked-base))
              (overlay-tags (resilience-context-tags overlay))
              (base-tags (resilience-context-tags checked-base))
              (overlay-baggage (resilience-context-baggage overlay))
              (base-baggage (resilience-context-baggage checked-base)))
          (make-resilience-context
           :operation (or overlay-operation base-operation)
           :correlation-id (or overlay-correlation-id base-correlation-id)
           :trace-id (or overlay-trace-id base-trace-id)
           :span-id (or overlay-span-id base-span-id)
           :parent-span-id (or overlay-parent-span-id base-parent-span-id)
           :idempotency-key (or overlay-idempotency-key
                                base-idempotency-key)
           :tags (nconc (copy-tree overlay-tags)
                        (copy-tree base-tags))
           :baggage (nconc (copy-tree overlay-baggage)
                           (copy-tree base-baggage)))))))

(defun %merge-resilience-context-operation (base operation idempotency-key)
  (if (null base)
      (make-resilience-context
       :operation operation
       :idempotency-key idempotency-key)
      (let ((checked-base base))
        (check-type checked-base resilience-context)
        (make-resilience-context
         :operation (or operation
                        (resilience-context-operation checked-base))
         :correlation-id
         (resilience-context-correlation-id checked-base)
         :trace-id
         (resilience-context-trace-id checked-base)
         :span-id
         (resilience-context-span-id checked-base)
         :parent-span-id
         (resilience-context-parent-span-id checked-base)
         :idempotency-key
         (or idempotency-key
             (resilience-context-idempotency-key checked-base))
         :tags
         (copy-tree (resilience-context-tags checked-base))
         :baggage
         (copy-tree (resilience-context-baggage checked-base))))))

(defmacro with-resilience-context ((&rest initargs) &body body)
  "Bind a merged RESILIENCE-CONTEXT for BODY."
  `(let ((*resilience-context*
           (merge-resilience-context
            *resilience-context*
            (make-resilience-context ,@initargs))))
     ,@body))

(defmacro with-resilience-event-handler ((handler) &body body)
  "Bind HANDLER as the inherited event sink for BODY.

HANDLER is evaluated once and may be NIL to explicitly disable inherited
events for the dynamic extent of BODY."
  `(let ((active-handler ,handler))
     (when active-handler
       (check-type active-handler function))
     (let ((*resilience-event-handler* active-handler))
       ,@body)))

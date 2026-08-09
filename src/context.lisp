(in-package #:cl-resilience-kit)

(defparameter +default-monotonic-units-per-second+
  (coerce internal-time-units-per-second 'double-float))

(defvar *resilience-clock* nil)
(defvar *resilience-monotonic-units-per-second* nil)
(defvar *resilience-deadline* nil)
(defvar *resilience-cancellation-token* nil)
(defvar *resilience-event-handler* nil)
(defvar *resilience-context* nil)

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

(defclass cancellation-token ()
  ((parent
    :initarg :parent
    :initform nil
    :reader cancellation-token-parent)
   (%cancelled-p
    :initform nil
    :accessor %cancellation-token-cancelled-p)
   (%reason
    :initform nil
    :accessor %cancellation-token-reason)
   (%lock
    :initarg :lock
    :reader %cancellation-token-lock)))

(defun make-cancellation-token (&key parent)
  "Create a cooperative cancellation token, optionally linked to PARENT.

Cancellation is observed only when CHECK-CANCELLATION-TOKEN is called.  A
parent token's cancellation is visible to every descendant token."
  (when parent
    (check-type parent cancellation-token))
  (make-instance
   'cancellation-token
   :parent parent
   :lock (cl-concurrent-kit:make-lock :name "cl-resilience-kit.cancellation-token")))

(defun %cancellation-token-local-state (token)
  (cl-concurrent-kit:with-lock-held ((%cancellation-token-lock token))
    (values (%cancellation-token-cancelled-p token)
            (%cancellation-token-reason token))))

(defun cancellation-token-cancelled-p (token)
  "Return true when TOKEN or one of its parents has been cancelled."
  (check-type token cancellation-token)
  (multiple-value-bind (cancelled-p reason)
      (%cancellation-token-local-state token)
    (declare (ignore reason))
    (or cancelled-p
        (let ((parent (cancellation-token-parent token)))
          (and parent (cancellation-token-cancelled-p parent))))))

(defun cancellation-token-reason (token)
  "Return the local cancellation reason, or the nearest parent reason."
  (check-type token cancellation-token)
  (multiple-value-bind (cancelled-p reason)
      (%cancellation-token-local-state token)
    (if cancelled-p
        reason
        (let ((parent (cancellation-token-parent token)))
          (and parent (cancellation-token-reason parent))))))

(defun cancel-cancellation-token (token &rest arguments)
  "Cancel TOKEN and return it.

The optional reason may be supplied positionally or as `:REASON`.  Repeated
cancellation preserves the first reason, which makes parent propagation
stable for observers."
  (check-type token cancellation-token)
  (let ((reason
          (cond ((null arguments) :cancelled)
                ((= (length arguments) 1) (first arguments))
                ((and (= (length arguments) 2)
                      (eq (first arguments) :reason))
                 (second arguments))
                (t
                 (error "CANCEL-CANCELLATION-TOKEN accepts an optional reason.")))))
    (cl-concurrent-kit:with-lock-held ((%cancellation-token-lock token))
      (unless (%cancellation-token-cancelled-p token)
        (setf (%cancellation-token-cancelled-p token) t
              (%cancellation-token-reason token) reason)))
    token))

(defun check-cancellation-token (token)
  "Signal RESILIENCE-CANCELLED when TOKEN or an ancestor is cancelled."
  (check-type token cancellation-token)
  (when (cancellation-token-cancelled-p token)
    (error 'resilience-cancelled
           :message (format nil "The resilience operation was cancelled~@[ (~A)~]."
                            (cancellation-token-reason token))
           :token token
           :reason (cancellation-token-reason token)))
  nil)

(defun %active-cancellation-token (token)
  (or token *resilience-cancellation-token*))

(defun %check-active-cancellation-token ()
  (when *resilience-cancellation-token*
    (check-cancellation-token *resilience-cancellation-token*)))

(defun %active-event-handler (handler)
  (or handler *resilience-event-handler*))

(defun %emit-resilience-event
    (handler type &key operation attempt stage condition result delay reason
                       timestamp context metadata duration clock
                       monotonic-units-per-second)
  "Invoke HANDLER with one RESILIENCE-EVENT, swallowing handler errors."
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
        (handler-case
            (funcall active-handler event)
          (error () nil))))
    nil))

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
adapter may copy these values into HTTP headers, message metadata, or a
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
   :lock (cl-concurrent-kit:make-lock
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
    (cl-concurrent-kit:with-lock-held ((%resilience-metrics-lock metrics))
      (incf (%resilience-metrics-total-events metrics))
      (incf (gethash key (%resilience-metrics-counts metrics) 0))
      (when (resilience-event-duration event)
        (incf (gethash key (%resilience-metrics-durations metrics) 0d0)
              (float (resilience-event-duration event) 1d0)))))
  event)

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
  "Return a fan-out handler.  One failing observer does not silence others."
  (check-type observer resilience-observer)
  (lambda (event)
    (dolist (handler (resilience-observer-handlers observer))
      (handler-case
          (funcall handler event)
        (error () nil)))
    event))

;;; Portable state store and fencing lease contracts

(defclass resilience-state-store () ())

(defgeneric state-store-get (store key))
(defgeneric state-store-put-if-version (store key value expected-version))
(defgeneric state-store-delete-if-version (store key expected-version))
(defgeneric state-store-scan-prefix (store prefix))

(defmethod state-store-get ((store resilience-state-store) key)
  (error 'resilience-store-error
         :key key
         :message "The state store has no GET implementation."))

(defmethod state-store-put-if-version
    ((store resilience-state-store) key value expected-version)
  (declare (ignore value expected-version))
  (error 'resilience-store-error
         :key key
         :message "The state store has no compare-and-set implementation."))

(defmethod state-store-delete-if-version
    ((store resilience-state-store) key expected-version)
  (declare (ignore expected-version))
  (error 'resilience-store-error
         :key key
         :message "The state store has no compare-and-delete implementation."))

(defmethod state-store-scan-prefix ((store resilience-state-store) prefix)
  (declare (ignore prefix))
  (error 'resilience-store-error
         :key nil
         :message "The state store has no prefix-scan implementation."))

(defstruct (%state-record
            (:constructor %make-state-record (version value)))
  version
  value)

(defclass memory-state-store (resilience-state-store)
  ((%records
    :initform (make-hash-table :test #'equal)
    :reader %memory-state-store-records)
   (%lock
    :initarg :lock
    :reader %memory-state-store-lock)))

(defun make-memory-state-store (&key name)
  (make-instance
   'memory-state-store
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.memory-state-store"))))

(defmethod state-store-get ((store memory-state-store) key)
  (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (let ((record (gethash key (%memory-state-store-records store))))
      (if record
          (values (copy-tree (%state-record-value record))
                  (%state-record-version record))
          (values nil nil)))))

(defmethod state-store-put-if-version
    ((store memory-state-store) key value expected-version)
  (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (let* ((records (%memory-state-store-records store))
           (record (gethash key records))
           (actual-version (and record (%state-record-version record))))
      (unless (eql expected-version actual-version)
        (error 'resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version actual-version
               :message "The state-store compare-and-set precondition failed."))
      (let ((new-version (1+ (or actual-version 0))))
        (setf (gethash key records)
              (%make-state-record new-version (copy-tree value)))
        new-version))))

(defmethod state-store-delete-if-version
    ((store memory-state-store) key expected-version)
  (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (let* ((records (%memory-state-store-records store))
           (record (gethash key records))
           (actual-version (and record (%state-record-version record))))
      (unless (eql expected-version actual-version)
        (error 'resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version actual-version
               :message "The state-store compare-and-delete precondition failed."))
      (remhash key records)
      t)))

(defmethod state-store-scan-prefix ((store memory-state-store) prefix)
  (check-type prefix string)
    (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (loop for key being the hash-keys of (%memory-state-store-records store)
          for record = (gethash key (%memory-state-store-records store))
          when (and (stringp key)
                    (<= (length prefix) (length key))
                    (string= prefix key :end2 (length prefix)))
            collect (list key
                          (copy-tree (%state-record-value record))
                          (%state-record-version record)))))

(defclass resilience-lease-store () ())

(defgeneric acquire-resilience-lease
    (store key owner &key ttl signal-on-unavailable-p))
(defgeneric renew-resilience-lease (lease &key ttl))
(defgeneric release-resilience-lease (lease &key ignore-lost-p))
(defgeneric resilience-lease-held-p (lease))

(defclass resilience-lease ()
  ((store
    :initarg :store
    :reader resilience-lease-store)
   (key
    :initarg :key
    :reader resilience-lease-key)
   (owner
    :initarg :owner
    :reader resilience-lease-owner)
   (fencing-token
    :initarg :fencing-token
    :reader resilience-lease-fencing-token)
   (expires-at
    :initarg :expires-at
    :accessor resilience-lease-expires-at)
   (ttl
    :initarg :ttl
    :accessor resilience-lease-ttl)))

(defstruct (%lease-record
            (:constructor %make-lease-record
                (owner fencing-token expires-at)))
  owner
  fencing-token
  expires-at)

(defclass memory-lease-store (resilience-lease-store)
  ((%leases
    :initform (make-hash-table :test #'equal)
    :reader %memory-lease-store-leases)
   (%next-fencing-token
    :initform (make-hash-table :test #'equal)
    :reader %memory-lease-store-next-fencing-token)
   (%lock
    :initarg :lock
    :reader %memory-lease-store-lock)
   (clock
    :initarg :clock
    :reader memory-lease-store-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader memory-lease-store-monotonic-units-per-second)))

(defun make-memory-lease-store
    (&key clock monotonic-units-per-second name)
  (make-instance
   'memory-lease-store
   :clock (%active-clock clock)
   :monotonic-units-per-second
   (%active-monotonic-units-per-second monotonic-units-per-second)
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.memory-lease-store"))))

(defun %memory-lease-now (store)
  (%now :clock (memory-lease-store-clock store)
        :monotonic-units-per-second
        (memory-lease-store-monotonic-units-per-second store)))

(defun %lease-record-matches-p (record lease now)
  (and record
       (equal (%lease-record-owner record) (resilience-lease-owner lease))
       (eql (%lease-record-fencing-token record)
            (resilience-lease-fencing-token lease))
       (> (%lease-record-expires-at record) now)))

(defmethod acquire-resilience-lease
    ((store memory-lease-store) key owner
     &key (ttl 30d0) (signal-on-unavailable-p t))
  (%ensure-positive-real ttl "TTL")
  (when (null owner)
    (error "LEASE owner must not be NIL."))
  (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
    (let* ((now (%memory-lease-now store))
           (leases (%memory-lease-store-leases store))
           (current (gethash key leases)))
      (cond
        ((and current
              (> (%lease-record-expires-at current) now)
              (not (equal owner (%lease-record-owner current))))
         (if signal-on-unavailable-p
             (error 'resilience-lease-unavailable
                    :key key
                    :owner owner
                    :retry-after
                    (max 0d0 (- (%lease-record-expires-at current) now))
                    :message "The resilience lease is held by another owner.")
             nil))
        (t
         (let* ((same-owner-p
                  (and current
                       (> (%lease-record-expires-at current) now)
                       (equal owner (%lease-record-owner current))))
                (fencing-token
                  (if same-owner-p
                      (%lease-record-fencing-token current)
                      (let ((next
                              (1+ (gethash key
                                          (%memory-lease-store-next-fencing-token store)
                                          0))))
                        (setf (gethash key
                                       (%memory-lease-store-next-fencing-token store))
                              next)
                        next)))
                (expires-at (+ now (float ttl 1d0))))
           (setf (gethash key leases)
                 (%make-lease-record owner fencing-token expires-at))
           (make-instance 'resilience-lease
                          :store store
                          :key key
                          :owner owner
                          :fencing-token fencing-token
                          :expires-at expires-at
                          :ttl (float ttl 1d0))))))))

(defmethod resilience-lease-held-p ((lease resilience-lease))
  (let ((store (resilience-lease-store lease)))
    (check-type store memory-lease-store)
    (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
      (%lease-record-matches-p
       (gethash (resilience-lease-key lease)
                (%memory-lease-store-leases store))
       lease
       (%memory-lease-now store)))))

(defmethod renew-resilience-lease ((lease resilience-lease) &key ttl)
  (let ((store (resilience-lease-store lease))
        (active-ttl (or ttl (resilience-lease-ttl lease))))
    (check-type store memory-lease-store)
    (%ensure-positive-real active-ttl "TTL")
    (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
      (let* ((now (%memory-lease-now store))
             (key (resilience-lease-key lease))
             (record (gethash key (%memory-lease-store-leases store))))
        (unless (%lease-record-matches-p record lease now)
          (error 'resilience-lease-lost
                 :key key
                 :owner (resilience-lease-owner lease)
                 :fencing-token (resilience-lease-fencing-token lease)
                 :message "The resilience lease can no longer be renewed."))
        (let ((expires-at (+ now (float active-ttl 1d0))))
          (setf (gethash key (%memory-lease-store-leases store))
                (%make-lease-record
                 (resilience-lease-owner lease)
                 (resilience-lease-fencing-token lease)
                 expires-at)
                (resilience-lease-expires-at lease) expires-at
                (resilience-lease-ttl lease) (float active-ttl 1d0))
          lease)))))

(defmethod release-resilience-lease
    ((lease resilience-lease) &key (ignore-lost-p nil))
  (let ((store (resilience-lease-store lease)))
    (check-type store memory-lease-store)
    (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
      (let* ((key (resilience-lease-key lease))
             (record (gethash key (%memory-lease-store-leases store))))
        (if (and record
                 (equal (%lease-record-owner record)
                        (resilience-lease-owner lease))
                 (eql (%lease-record-fencing-token record)
                      (resilience-lease-fencing-token lease)))
            (progn
              (remhash key (%memory-lease-store-leases store))
              t)
            (if ignore-lost-p
                nil
                (error 'resilience-lease-lost
                       :key key
                       :owner (resilience-lease-owner lease)
                       :fencing-token (resilience-lease-fencing-token lease)
                       :message "The resilience lease is no longer owned.")))))))

(defmacro with-resilience-lease
    ((lease store key owner &rest options) &body body)
  "Acquire LEASE for BODY and release it on every exit path."
  `(let ((,lease (acquire-resilience-lease ,store ,key ,owner
                                            ,@options)))
     (when ,lease
       (unwind-protect
            (progn ,@body)
         (release-resilience-lease ,lease :ignore-lost-p t)))))

;;; Lifecycle and health boundaries

(defclass resilience-lifecycle ()
  ((%state
    :initform :running
    :accessor %resilience-lifecycle-state)
   (%active
    :initform 0
    :accessor %resilience-lifecycle-active)
   (%lock
    :initarg :lock
    :reader %resilience-lifecycle-lock)
   (%condition-variable
    :initarg :condition-variable
    :reader %resilience-lifecycle-condition-variable)))

(defun make-resilience-lifecycle (&key name)
  (make-instance
   'resilience-lifecycle
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.lifecycle"))
   :condition-variable
   (cl-concurrent-kit:make-condition-variable
    :name (or name "cl-resilience-kit.lifecycle.condition"))))

(defun resilience-lifecycle-state (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (%resilience-lifecycle-state lifecycle)))

(defun resilience-lifecycle-active (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (%resilience-lifecycle-active lifecycle)))

(defun resilience-lifecycle-accepting-p (lifecycle)
  (eq (resilience-lifecycle-state lifecycle) :running))

(defun begin-resilience-drain (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (when (eq (%resilience-lifecycle-state lifecycle) :running)
      (setf (%resilience-lifecycle-state lifecycle) :draining)
      (cl-concurrent-kit:condition-broadcast
       (%resilience-lifecycle-condition-variable lifecycle)))
    (%resilience-lifecycle-state lifecycle)))

(defun enter-resilience-lifecycle (lifecycle &key operation)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (unless (eq (%resilience-lifecycle-state lifecycle) :running)
      (error 'resilience-draining
             :operation operation
             :state (%resilience-lifecycle-state lifecycle)
             :message "The resilience lifecycle is draining or stopped."))
    (incf (%resilience-lifecycle-active lifecycle))
    t))

(defun leave-resilience-lifecycle (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (when (plusp (%resilience-lifecycle-active lifecycle))
      (decf (%resilience-lifecycle-active lifecycle)))
    (when (zerop (%resilience-lifecycle-active lifecycle))
      (cl-concurrent-kit:condition-broadcast
       (%resilience-lifecycle-condition-variable lifecycle)))
    (%resilience-lifecycle-active lifecycle)))

(defun await-resilience-drained (lifecycle &key timeout)
  "Wait until no operation remains active; return NIL on TIMEOUT."
  (check-type lifecycle resilience-lifecycle)
  (when timeout
    (%ensure-non-negative-real timeout "TIMEOUT"))
  (let ((deadline (and timeout (+ (%now) (float timeout 1d0)))))
    (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
      (loop while (plusp (%resilience-lifecycle-active lifecycle)) do
        (let ((remaining (and deadline (max 0d0 (- deadline (%now))))))
          (when (and deadline (zerop remaining))
            (return-from await-resilience-drained nil))
          (unless (cl-concurrent-kit:condition-wait
                   (%resilience-lifecycle-condition-variable lifecycle)
                   (%resilience-lifecycle-lock lifecycle)
                   :timeout remaining)
            (when (and deadline
                       (plusp (%resilience-lifecycle-active lifecycle)))
              (return-from await-resilience-drained nil)))))
      t)))

(defun stop-resilience-lifecycle (lifecycle &key timeout)
  (check-type lifecycle resilience-lifecycle)
  (begin-resilience-drain lifecycle)
  (when (await-resilience-drained lifecycle :timeout timeout)
    (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
      (setf (%resilience-lifecycle-state lifecycle) :stopped)
      (cl-concurrent-kit:condition-broadcast
       (%resilience-lifecycle-condition-variable lifecycle)))
    t))

(defmacro with-resilience-lifecycle ((lifecycle &key operation) &body body)
  `(progn
     (enter-resilience-lifecycle ,lifecycle :operation ,operation)
     (unwind-protect
          (progn ,@body)
       (leave-resilience-lifecycle ,lifecycle))))

(defclass health-registry ()
  ((%checks
    :initform (make-hash-table :test #'equal)
    :reader %health-registry-checks)
   (%lock
    :initarg :lock
    :reader %health-registry-lock)))

(defun make-health-registry (&key name)
  (make-instance
   'health-registry
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.health"))))

(defun register-health-check (registry name checker)
  (check-type registry health-registry)
  (check-type checker function)
  (cl-concurrent-kit:with-lock-held ((%health-registry-lock registry))
    (setf (gethash name (%health-registry-checks registry)) checker))
  name)

(defun unregister-health-check (registry name)
  (check-type registry health-registry)
  (cl-concurrent-kit:with-lock-held ((%health-registry-lock registry))
    (remhash name (%health-registry-checks registry)))
  registry)

(defun health-report (registry)
  (check-type registry health-registry)
  (let ((checks
          (cl-concurrent-kit:with-lock-held ((%health-registry-lock registry))
            (loop for name being the hash-keys of (%health-registry-checks registry)
                  using (hash-value checker)
                  collect (cons name checker)))))
    (mapcar
     (lambda (entry)
       (handler-case
           (let ((value (funcall (cdr entry))))
             (list :name (car entry)
                   :status (if value :healthy :unhealthy)
                   :value value))
         (error (condition)
           (list :name (car entry)
                 :status :unhealthy
                 :condition condition))))
     checks)))

(defun health-ready-p (registry)
  (let ((report (health-report registry)))
    (and report
         (every (lambda (entry) (eq (getf entry :status) :healthy)) report))))

(defun health-live-p (registry)
  (declare (ignore registry))
  t)

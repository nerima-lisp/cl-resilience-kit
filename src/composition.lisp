(in-package #:cl-resilience-kit)

(defun %call-with-resilience-core
    (thunk &key retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
                 (rate-limit-tokens 1d0) rate-limit-wait-p rate-limit-max-wait
                 (rate-limit-signal-on-reject-p t)
                 overall-timeout overall-deadline per-attempt-timeout
                 clock monotonic-units-per-second sleeper operation
                 retry-budget cancellation-token event-handler fallback)
  "Compose the resilience controls around THUNK.

The fixed nesting order is BULKHEAD (the complete logical operation) -> RETRY
(the attempt loop) -> RATE-LIMITER (each attempt) -> CIRCUIT-BREAKER (each
attempt) -> THUNK.  The supplied clock, unit scale, and sleeper should be the
same boundary objects used by the configured primitives when a deterministic
composition is required.

When RATE-LIMITER is present, rejection signals RATE-LIMIT-EXCEEDED by
default.  Set RATE-LIMIT-SIGNAL-ON-REJECT-P to NIL to return
`(VALUES NIL RETRY-AFTER)` from the composed operation without signaling.
The retry result classifier receives the first value, NIL; use signal mode
when the retry policy needs to classify RATE-LIMIT-EXCEEDED and its
retry-after hint."
  (check-type thunk function)
  (let* ((active-token (%active-cancellation-token cancellation-token))
         (attempt-thunk
          (lambda ()
            (flet ((run-breaker ()
                     (if circuit-breaker
                         (circuit-breaker-call circuit-breaker thunk
                                               :operation operation
                                               :cancellation-token
                                               active-token
                                               :event-handler event-handler)
                         (funcall thunk))))
              (if rate-limiter
                  (multiple-value-bind (acquired-p retry-after)
                      (rate-limiter-acquire
                       rate-limiter
                       :tokens rate-limit-tokens
                       :wait-p rate-limit-wait-p
                       :max-wait rate-limit-max-wait
                       :signal-on-reject-p rate-limit-signal-on-reject-p
                       :operation operation
                       :cancellation-token active-token
                       :event-handler event-handler)
                    (if acquired-p
                        (run-breaker)
                        (values nil retry-after)))
                  (run-breaker)))))
        (policy (or retry-policy
                    (make-retry-policy :max-attempts 1))))
    (flet ((run-chain ()
             (call-with-retry
              policy attempt-thunk
              :overall-timeout overall-timeout
              :overall-deadline overall-deadline
              :per-attempt-timeout per-attempt-timeout
              :clock clock
              :monotonic-units-per-second monotonic-units-per-second
              :sleeper sleeper
              :operation operation
              :retry-budget retry-budget
              :cancellation-token active-token
              :event-handler event-handler
              :fallback fallback)))
      (if bulkhead
          (progn
            ;; The bulkhead wraps the complete logical operation. Its
            ;; post-return cancellation check must not turn a value returned
            ;; by RETRY's fallback into a second cancellation. Check before
            ;; admission, then let the inner retry boundary own the token.
            (when active-token
              (check-cancellation-token active-token))
            (let ((*resilience-cancellation-token* nil))
              (bulkhead-call bulkhead #'run-chain
                             :operation operation
                             :cancellation-token nil
                             :event-handler event-handler
                             :timeout bulkhead-timeout)))
          (run-chain)))))

(defmacro with-resilience ((&rest options) &body body)
  "Evaluate BODY using CALL-WITH-RESILIENCE's keyword options."
  `(call-with-resilience (lambda () ,@body) ,@options))

(defun %seconds->duration (seconds)
  (cl-date-kit:duration-of-nanos
   (round (* (coerce seconds 'double-float) 1000000000d0))))

(defclass resilience-executor ()
  ((implementation
    :initarg :implementation
    :reader resilience-executor-implementation)))

(defun make-resilience-executor
    (&key (size 4) name queue-capacity)
  "Create an executor with resilience-specific rejection and value handling."
  (check-type size (integer 1 *))
  (when queue-capacity
    (check-type queue-capacity (integer 1 *)))
  (make-instance 'resilience-executor
                 :implementation
                 (cl-concurrent-kit:make-executor
                  :size size
                  :name (or name "cl-resilience-kit")
                  :queue-capacity queue-capacity)))

(defun resilience-executor-queue-depth (executor)
  (check-type executor resilience-executor)
  (cl-concurrent-kit:executor-queue-depth
   (resilience-executor-implementation executor)))

(defun resilience-executor-queue-capacity (executor)
  (check-type executor resilience-executor)
  (cl-concurrent-kit:executor-queue-capacity
   (resilience-executor-implementation executor)))

(defun resilience-executor-high-water-mark (executor)
  (check-type executor resilience-executor)
  (cl-concurrent-kit:executor-high-water-mark
   (resilience-executor-implementation executor)))

(defun resilience-executor-shutdown-p (executor)
  (check-type executor resilience-executor)
  (cl-concurrent-kit:executor-shutdown-p
   (resilience-executor-implementation executor)))

(defun resilience-executor-terminated-p (executor)
  (check-type executor resilience-executor)
  (cl-concurrent-kit:executor-terminated-p
   (resilience-executor-implementation executor)))

(defun %make-hard-timeout-condition
    (operation timeout backend started &key clock monotonic-units-per-second)
  (make-condition
   'resilience-hard-timeout
   :operation operation
   :message (format nil "The ~A execution backend exceeded its hard timeout of ~A seconds."
                    backend timeout)
   :deadline (+ started timeout)
   :observed-at (%now :clock clock
                      :monotonic-units-per-second
                      monotonic-units-per-second)
   :stage :execution
   :attempt nil
   :timeout timeout
   :backend backend))

(defun %run-with-hard-timeout
    (thunk timeout operation backend &key clock monotonic-units-per-second)
  (check-type thunk function)
  (if (null timeout)
      (funcall thunk)
      (let ((timeout (%ensure-non-negative-real timeout "HARD-TIMEOUT"))
            (started (%now :clock clock
                           :monotonic-units-per-second
                           monotonic-units-per-second)))
        (when (zerop timeout)
          (error (%make-hard-timeout-condition
                  operation timeout backend started
                  :clock clock
                  :monotonic-units-per-second
                  monotonic-units-per-second)))
        (handler-case
            (cl-concurrent-kit:with-timeout (%seconds->duration timeout)
              (funcall thunk))
          (cl-concurrent-kit:operation-timed-out (condition)
            (if (eq (cl-concurrent-kit:operation-timed-out-operation condition)
                    :with-timeout)
                (error (%make-hard-timeout-condition
                        operation timeout backend started
                        :clock clock
                        :monotonic-units-per-second
                        monotonic-units-per-second))
                (error condition)))))))

(defun resilience-executor-try-submit
    (executor thunk &key hard-timeout operation clock monotonic-units-per-second)
  "Submit THUNK and return PROMISE and an acceptance boolean."
  (check-type executor resilience-executor)
  (check-type thunk function)
  (multiple-value-bind (promise accepted-p)
      (cl-concurrent-kit:try-submit
       (resilience-executor-implementation executor)
       (lambda ()
         (multiple-value-list
          (%run-with-hard-timeout
           thunk hard-timeout operation :executor
           :clock clock
           :monotonic-units-per-second
           monotonic-units-per-second))))
    (values promise accepted-p)))

(defun resilience-executor-submit
    (executor thunk &key hard-timeout operation clock monotonic-units-per-second)
  "Submit THUNK, signaling RESILIENCE-EXECUTION-REJECTED when refused."
  (multiple-value-bind (promise accepted-p)
      (resilience-executor-try-submit executor thunk
                                      :hard-timeout hard-timeout
                                      :operation operation
                                      :clock clock
                                      :monotonic-units-per-second
                                      monotonic-units-per-second)
    (if accepted-p
        promise
        (error 'resilience-execution-rejected
               :operation operation
               :message "The resilience executor rejected the execution."
               :reason :executor-rejected
               :queue-size (resilience-executor-queue-depth executor)))))

(defun resilience-executor-call
    (executor thunk &key hard-timeout timeout operation clock
                                         monotonic-units-per-second)
  "Run THUNK on EXECUTOR and return all of its values.

TIMEOUT bounds the caller's wait.  HARD-TIMEOUT bounds execution in the
worker, but arbitrary interruption remains subject to the backend's safety
constraints."
  (check-type executor resilience-executor)
  (check-type thunk function)
  (when hard-timeout
    (%ensure-non-negative-real hard-timeout "HARD-TIMEOUT"))
  (when timeout
    (%ensure-non-negative-real timeout "EXECUTOR-TIMEOUT"))
  (let* ((wait-timeout timeout)
         (promise (resilience-executor-submit
                   executor thunk
                   :hard-timeout hard-timeout
                   :operation operation
                   :clock clock
                   :monotonic-units-per-second
                   monotonic-units-per-second)))
    (%await-resilience-promise
     promise wait-timeout
     :operation operation
     :timeout-backend :executor-wait)))

(defun resilience-executor-shutdown
    (executor &key wait cancel-pending timeout)
  (check-type executor resilience-executor)
  (when timeout
    (%ensure-non-negative-real timeout "EXECUTOR-SHUTDOWN-TIMEOUT"))
  (cl-concurrent-kit:shutdown-executor
   (resilience-executor-implementation executor)
   :wait wait
   :cancel-pending cancel-pending
   :timeout (and timeout (%seconds->duration timeout))))

(defun %make-execution-timeout-condition (operation timeout backend)
  (make-condition
   'resilience-execution-timeout
   :operation operation
   :message (format nil "The ~A caller wait exceeded its timeout of ~A seconds."
                    backend timeout)
   :timeout timeout
   :backend backend))

(defun %await-resilience-promise
    (promise timeout &key operation timeout-backend)
  (handler-case
      (let ((values
              (if timeout
                  (cl-concurrent-kit:await
                   promise :timeout (%seconds->duration timeout))
                  (cl-concurrent-kit:await promise))))
        (if (listp values)
            (values-list values)
            (values values)))
    (cl-concurrent-kit:operation-timed-out (condition)
      (if (and timeout-backend
               (eq (cl-concurrent-kit:operation-timed-out-operation
                    condition)
                   :await))
          (error (%make-execution-timeout-condition
                  operation timeout timeout-backend))
          (error condition)))))

(defun %submit-resilience-promise
    (thunk &key executor hard-timeout operation clock monotonic-units-per-second)
  (if executor
      (resilience-executor-submit executor thunk
                                  :hard-timeout hard-timeout
                                  :operation operation
                                  :clock clock
                                  :monotonic-units-per-second
                                  monotonic-units-per-second)
      (cl-concurrent-kit:future
        (multiple-value-list
         (%run-with-hard-timeout
          thunk hard-timeout operation :thread
          :clock clock
          :monotonic-units-per-second
          monotonic-units-per-second)))))

(defun call-with-hedging
    (thunk &key (hedge-after 0d0) (max-attempts 2) executor hard-timeout
                 hedge-safe-p idempotency-key cancellation-token operation
                 clock monotonic-units-per-second)
  "Run an idempotent THUNK with delayed parallel hedge attempts.

The operation must opt in through HEDGE-SAFE-P or provide an idempotency key.
Loser attempts are not forcefully cancelled by the underlying promise API."
  (check-type thunk function)
  (check-type max-attempts (integer 1 *))
  (%ensure-non-negative-real hedge-after "HEDGE-AFTER")
  (let ((key (or idempotency-key
                 (and (current-resilience-context)
                      (resilience-context-idempotency-key
                       (current-resilience-context))))))
    (when (and (> max-attempts 1)
               (not (or hedge-safe-p key)))
      (error 'hedge-unsafe
             :operation operation
             :message "Hedging requires an explicitly safe operation or an idempotency key."
             :reason :non-idempotent-operation))
    (when cancellation-token
      (check-cancellation-token cancellation-token))
    (when (= max-attempts 1)
      (return-from call-with-hedging
        (if executor
            (%await-resilience-promise
             (%submit-resilience-promise
              thunk
              :executor executor
              :hard-timeout hard-timeout
              :operation operation
              :clock clock
              :monotonic-units-per-second
              monotonic-units-per-second)
             nil)
            (%run-with-hard-timeout
             thunk hard-timeout operation :thread
             :clock clock
             :monotonic-units-per-second
             monotonic-units-per-second))))
    (let ((promises nil)
          (causes nil))
      (labels ((launch ()
                 (when cancellation-token
                   (check-cancellation-token cancellation-token))
                 (push (%submit-resilience-promise
                        thunk
                        :executor executor
                        :hard-timeout hard-timeout
                        :operation operation
                        :clock clock
                        :monotonic-units-per-second
                        monotonic-units-per-second)
                       promises)))
        (launch)
        (unless (zerop hedge-after)
          (handler-case
              (return-from call-with-hedging
                (%await-resilience-promise (first promises) hedge-after))
            (cl-concurrent-kit:operation-timed-out (condition)
              (if (eq (cl-concurrent-kit:operation-timed-out-operation
                       condition)
                      :await)
                  nil
                  (error condition)))
            (resilience-cancelled (condition)
              (error condition))
            (error (condition)
              (push condition causes))))
        (loop repeat (1- max-attempts) do (launch))
        (handler-case
            (%await-resilience-promise
             (cl-concurrent-kit:promise-any (nreverse promises))
             nil)
          (cl-concurrent-kit:promise-all-failed (condition)
            (error 'hedge-exhausted
                   :operation operation
                   :message "All hedge attempts failed."
                   :causes (append (nreverse causes)
                                   (cl-concurrent-kit:promise-all-failed-causes
                                    condition))
                   :attempts max-attempts)))))))

(defclass request-coalescer ()
  ((lock
    :initform (cl-concurrent-kit:make-lock
               :name "resilience-request-coalescer")
    :reader %request-coalescer-lock)
   (entries
    :initform (make-hash-table :test #'equal)
    :reader %request-coalescer-entries)))

(defun make-request-coalescer ()
  (make-instance 'request-coalescer))

(defun request-coalescer-size (coalescer)
  (check-type coalescer request-coalescer)
  (cl-concurrent-kit:with-lock-held ((%request-coalescer-lock coalescer))
    (hash-table-count (%request-coalescer-entries coalescer))))

(defun %remove-coalesced-request (coalescer key promise)
  (cl-concurrent-kit:with-lock-held ((%request-coalescer-lock coalescer))
    (let ((entry (gethash key (%request-coalescer-entries coalescer))))
      (when (and entry (eq (getf entry :promise) promise))
        (remhash key (%request-coalescer-entries coalescer))))))

(defun call-with-request-coalescing
    (coalescer thunk &key key idempotency-fingerprint executor hard-timeout
                         timeout operation clock monotonic-units-per-second)
  "Share one in-flight result for the same idempotency KEY.

This adapter is process-local.  A non-equal fingerprint for an in-flight key
signals IDempotency-CONFLICT instead of joining an ambiguous operation."
  (check-type coalescer request-coalescer)
  (check-type thunk function)
  (let ((key (or key
                 (and (current-resilience-context)
                      (resilience-context-idempotency-key
                       (current-resilience-context))))))
    (unless key
      (error 'idempotency-key-required
             :operation operation
             :message "Request coalescing requires an idempotency key."))
    (let ((owner-p nil)
          (promise nil))
      (cl-concurrent-kit:with-lock-held ((%request-coalescer-lock coalescer))
        (let ((entry (gethash key (%request-coalescer-entries coalescer))))
          (if entry
              (progn
                (when (or (and (getf entry :fingerprint)
                               (not (equal (getf entry :fingerprint)
                                           idempotency-fingerprint)))
                          (and (null (getf entry :fingerprint))
                               idempotency-fingerprint))
                  (error 'idempotency-conflict
                         :operation operation
                         :message "An in-flight idempotency key has a different fingerprint."
                         :key key
                         :existing-value (getf entry :fingerprint)))
                (setf promise (getf entry :promise)))
              (progn
                (setf promise (cl-concurrent-kit:make-promise)
                      owner-p t)
                (setf (gethash key (%request-coalescer-entries coalescer))
                      (list :promise promise
                            :fingerprint idempotency-fingerprint))))))
      (when owner-p
        (let ((worker
                (lambda ()
                  (let ((settled-p nil))
                    (unwind-protect
                         (handler-case
                             (progn
                               (cl-concurrent-kit:deliver
                                promise
                                (multiple-value-list
                                 (%run-with-hard-timeout
                                  thunk hard-timeout operation :coalescer
                                  :clock clock
                                  :monotonic-units-per-second
                                  monotonic-units-per-second)))
                               (setf settled-p t))
                           (control-error (condition)
                             (declare (ignore condition)))
                           (error (condition)
                             (cl-concurrent-kit:deliver-error promise condition)
                             (setf settled-p t)))
                      (unless settled-p
                        (cl-concurrent-kit:deliver-error
                         promise
                         (make-condition
                          'resilience-error
                          :operation operation
                          :message "The coalesced operation exited without settling its promise.")))
                      (%remove-coalesced-request coalescer key promise))))))
          (handler-case
              (if executor
                  (resilience-executor-submit executor worker
                                              :operation operation
                                              :clock clock
                                              :monotonic-units-per-second
                                              monotonic-units-per-second)
                  (cl-concurrent-kit:future (funcall worker)))
            (error (condition)
              (%remove-coalesced-request coalescer key promise)
              (cl-concurrent-kit:deliver-error promise condition)))))
      (%await-resilience-promise
       promise timeout
       :operation operation
       :timeout-backend :coalescer-wait))))

(defun %combined-resilience-handler (event-handler metrics observer)
  (let ((handlers
          (remove nil
                  (list event-handler
                        (and metrics (resilience-metrics-handler metrics))
                        (and observer (resilience-observer-handler observer))))))
    (cond ((null handlers) nil)
          ((null (rest handlers)) (first handlers))
          (t (lambda (event)
               (dolist (handler handlers)
                 (funcall handler event)))))))

(defun call-with-resilience
    (thunk &key retry-policy circuit-breaker distributed-circuit-breaker
                 bulkhead bulkhead-timeout rate-limiter
                 (rate-limit-tokens 1d0) rate-limit-wait-p rate-limit-max-wait
                 (rate-limit-signal-on-reject-p t)
                 overall-timeout overall-deadline per-attempt-timeout
                 clock monotonic-units-per-second sleeper operation
                 retry-budget cancellation-token event-handler fallback
                 context metrics observer lifecycle executor executor-timeout
                 hard-timeout hedge-after max-hedge-attempts hedge-safe-p
                 request-coalescer idempotency-key idempotency-fingerprint)
  "Compose local and distributed resilience controls around THUNK.

The existing retry/bulkhead/rate-limit/circuit-breaker contract is preserved.
EXECUTOR, HARD-TIMEOUT, HEDGE-AFTER, and REQUEST-COALESCER add optional
execution-boundary controls; hedging and coalescing require idempotency
protection and do not cancel already running loser work."
  (check-type thunk function)
  (when context
    (check-type context resilience-context))
  (when lifecycle
    (check-type lifecycle resilience-lifecycle))
  (when executor
    (check-type executor resilience-executor))
  (let* ((base-context
           (merge-resilience-context
            (current-resilience-context)
            (make-resilience-context
             :operation operation
             :idempotency-key idempotency-key)))
         (active-context (if context
                            (merge-resilience-context base-context context)
                            base-context))
         (active-handler (%combined-resilience-handler
                          event-handler metrics observer))
         (active-token (%active-cancellation-token cancellation-token))
         (entered-p nil)
         (started (%now :clock clock
                        :monotonic-units-per-second
                        monotonic-units-per-second)))
    (when lifecycle
      (enter-resilience-lifecycle lifecycle :operation operation)
      (setf entered-p t))
    (unwind-protect
         (let ((*resilience-context* active-context)
               (*resilience-event-handler* active-handler))
           (labels ((run-base ()
                      (let ((operation-thunk
                              (lambda ()
                                (%call-with-resilience-core
                                 thunk
                                 :retry-policy retry-policy
                                 :circuit-breaker circuit-breaker
                                 :bulkhead bulkhead
                                 :bulkhead-timeout bulkhead-timeout
                                 :rate-limiter rate-limiter
                                 :rate-limit-tokens rate-limit-tokens
                                 :rate-limit-wait-p rate-limit-wait-p
                                 :rate-limit-max-wait rate-limit-max-wait
                                 :rate-limit-signal-on-reject-p
                                 rate-limit-signal-on-reject-p
                                 :overall-timeout overall-timeout
                                 :overall-deadline overall-deadline
                                 :per-attempt-timeout per-attempt-timeout
                                 :clock clock
                                 :monotonic-units-per-second
                                 monotonic-units-per-second
                                 :sleeper sleeper
                                 :operation operation
                                 :retry-budget retry-budget
                                 :cancellation-token active-token
                                 :event-handler active-handler
                                 :fallback fallback))))
                        (if distributed-circuit-breaker
                            (distributed-circuit-breaker-call
                             distributed-circuit-breaker operation-thunk
                             :operation operation
                             :cancellation-token active-token
                             :event-handler active-handler)
                            (funcall operation-thunk))))
                    (run-execution ()
                      (cond
                        (request-coalescer
                         (call-with-request-coalescing
                          request-coalescer #'run-base
                          :key idempotency-key
                          :idempotency-fingerprint idempotency-fingerprint
                          :executor executor
                          :hard-timeout hard-timeout
                          :timeout (or executor-timeout overall-timeout)
                          :operation operation
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        ((and max-hedge-attempts (> max-hedge-attempts 1))
                         (call-with-hedging
                          #'run-base
                          :hedge-after (or hedge-after 0d0)
                          :max-attempts max-hedge-attempts
                          :executor executor
                          :hard-timeout hard-timeout
                          :hedge-safe-p hedge-safe-p
                          :idempotency-key idempotency-key
                          :cancellation-token active-token
                          :operation operation
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        (executor
                         (resilience-executor-call
                          executor #'run-base
                          :hard-timeout hard-timeout
                          :timeout (or executor-timeout overall-timeout)
                          :operation operation
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        (hard-timeout
                         (%run-with-hard-timeout
                          #'run-base hard-timeout operation :thread
                          :clock clock
                          :monotonic-units-per-second
                          monotonic-units-per-second))
                        (t (run-base)))))
             (handler-case
                 (multiple-value-prog1
                     (run-execution)
                   (%emit-resilience-event
                    active-handler :operation-complete
                    :operation operation
                    :stage :composition
                    :context active-context
                    :duration (- (%now :clock clock
                                       :monotonic-units-per-second
                                       monotonic-units-per-second)
                                 started)
                    :clock clock
                    :monotonic-units-per-second
                    monotonic-units-per-second))
               (error (condition)
                 (%emit-resilience-event
                  active-handler :operation-failed
                  :operation operation
                  :stage :composition
                  :condition condition
                  :context active-context
                  :duration (- (%now :clock clock
                                     :monotonic-units-per-second
                                     monotonic-units-per-second)
                               started)
                  :clock clock
                  :monotonic-units-per-second
                  monotonic-units-per-second)
                 (error condition)))))
      (when entered-p
        (leave-resilience-lifecycle lifecycle)))))

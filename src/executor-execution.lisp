(in-package #:resilience-kit)

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

(defun %timeout-operation-matches-p (condition expected-operation)
  (eq (operation-timed-out-operation condition)
      expected-operation))

(defun %rethrow-unless-timeout-operation
    (condition expected-operation replacement-condition)
  (unless (%timeout-operation-matches-p condition expected-operation)
    (error condition))
  (error replacement-condition))

(defun %run-with-hard-timeout
    (thunk timeout operation backend &key clock monotonic-units-per-second)
  (check-type thunk function)
  (unless timeout
    (return-from %run-with-hard-timeout
      (funcall thunk)))
  (let* ((timeout (%ensure-non-negative-real timeout "HARD-TIMEOUT"))
         (active-clock (%active-clock clock))
         (units (%active-monotonic-units-per-second
                 monotonic-units-per-second))
         (started (%monotonic-seconds active-clock units)))
    (when (zerop timeout)
      (error (%make-hard-timeout-condition
              operation timeout backend started
              :clock active-clock
              :monotonic-units-per-second units)))
    (handler-case
        (with-timeout (%seconds->duration timeout)
          (funcall thunk))
      (operation-timed-out (condition)
        (if (%timeout-operation-matches-p condition :with-timeout)
            (error (%make-hard-timeout-condition
                    operation timeout backend started
                    :clock active-clock
                    :monotonic-units-per-second units))
            (error condition))))))

(defun %make-resilience-task-runner
    (thunk &key hard-timeout operation backend clock monotonic-units-per-second)
  (check-type thunk function)
  (lambda ()
    (%pack-resilience-values
     (if hard-timeout
         (%run-with-hard-timeout
          thunk hard-timeout operation backend
          :clock clock
          :monotonic-units-per-second monotonic-units-per-second)
         (funcall thunk)))))

(defun resilience-executor-try-submit
    (executor thunk &key hard-timeout operation clock monotonic-units-per-second)
  "Submit THUNK and return PROMISE and an acceptance boolean."
  (check-type executor resilience-executor)
  (check-type thunk function)
  (let ((implementation (%resilience-executor-implementation executor)))
    (multiple-value-bind (promise accepted-p)
        (try-submit
         implementation
         (%make-resilience-task-runner
          thunk
          :hard-timeout hard-timeout
          :operation operation
          :backend :executor
          :clock clock
          :monotonic-units-per-second monotonic-units-per-second))
      (values promise accepted-p))))

(defun resilience-executor-submit
    (executor thunk &key hard-timeout operation clock monotonic-units-per-second)
  "Submit THUNK, signaling RESILIENCE-EXECUTION-REJECTED when refused."
  (check-type executor resilience-executor)
  (check-type thunk function)
  (let ((implementation (%resilience-executor-implementation executor)))
    (multiple-value-bind (promise accepted-p)
        (try-submit
         implementation
         (%make-resilience-task-runner
          thunk
          :hard-timeout hard-timeout
          :operation operation
          :backend :executor
          :clock clock
          :monotonic-units-per-second monotonic-units-per-second))
      (if accepted-p
          promise
          (error 'resilience-execution-rejected
                 :operation operation
                 :message "The resilience executor rejected the execution."
                 :reason :executor-rejected
                 :queue-size (executor-queue-depth implementation))))))

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
  (shutdown-executor
   (%resilience-executor-implementation executor)
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
                  (await
                   promise :timeout (%seconds->duration timeout))
                  (await promise))))
        (%unpack-resilience-values values))
    (operation-timed-out (condition)
      (unless timeout-backend
        (error condition))
      (%rethrow-unless-timeout-operation
       condition :await
       (%make-execution-timeout-condition
        operation timeout timeout-backend)))))

(defun %submit-resilience-promise
    (thunk &key executor hard-timeout operation clock monotonic-units-per-second)
  (if executor
      (resilience-executor-submit executor thunk
                                  :hard-timeout hard-timeout
                                  :operation operation
                                  :clock clock
                                  :monotonic-units-per-second
                                  monotonic-units-per-second)
      (let ((worker (%make-resilience-task-runner
                     thunk
                     :hard-timeout hard-timeout
                     :operation operation
                     :backend :thread
                     :clock clock
                     :monotonic-units-per-second
                     monotonic-units-per-second)))
        (future (funcall worker)))))

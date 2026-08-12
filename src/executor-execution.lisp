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
        (with-timeout (%seconds->duration timeout)
          (funcall thunk))
      (operation-timed-out (condition)
        (%rethrow-unless-timeout-operation
         condition :with-timeout
         (%make-hard-timeout-condition
          operation timeout backend started
          :clock clock
          :monotonic-units-per-second
          monotonic-units-per-second))))))

(defun resilience-executor-try-submit
    (executor thunk &key hard-timeout operation clock monotonic-units-per-second)
  "Submit THUNK and return PROMISE and an acceptance boolean."
  (check-type executor resilience-executor)
  (check-type thunk function)
  (multiple-value-bind (promise accepted-p)
      (try-submit
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
  (shutdown-executor
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
                  (await
                   promise :timeout (%seconds->duration timeout))
                  (await promise))))
        (if (listp values)
            (values-list values)
            (values values)))
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
      (future
        (multiple-value-list
         (%run-with-hard-timeout
          thunk hard-timeout operation :thread
          :clock clock
          :monotonic-units-per-second
          monotonic-units-per-second)))))

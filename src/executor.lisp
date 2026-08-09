(in-package #:cl-resilience-kit)

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

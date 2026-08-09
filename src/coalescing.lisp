(in-package #:cl-resilience-kit)

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

This execution layer is process-local.  A non-equal fingerprint for an in-flight key
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

(in-package #:resilience-kit)

(defun %deliver-coalesced-error (promise condition)
  "Deliver CONDITION unless another path has already settled PROMISE."
  (handler-case
      (deliver-error promise condition)
    (promise-already-fulfilled ()
      nil)))

(defun %remove-coalesced-request (coalescer key promise)
  (with-lock-held ((%request-coalescer-lock coalescer))
    (let* ((entries (%request-coalescer-entries coalescer))
           (entry (gethash key entries)))
      (when (and entry
                 (eq (%coalesced-request-promise entry) promise))
        (remhash key entries)))))

(defun call-with-request-coalescing
    (coalescer thunk &key key idempotency-fingerprint executor hard-timeout
                         timeout operation clock monotonic-units-per-second)
  "Share one in-flight result for the same idempotency KEY.

This execution layer is process-local. A non-equal fingerprint for an in-flight key
signals IDEMPOTENCY-CONFLICT instead of joining an ambiguous operation."
  (check-type coalescer request-coalescer)
  (check-type thunk function)
  (let* ((active-context (current-resilience-context))
         (key (or key
                  (and active-context
                       (resilience-context-idempotency-key active-context)))))
    (unless key
      (error 'idempotency-key-required
             :operation operation
             :message "Request coalescing requires an idempotency key."))
    (let ((owner-p nil)
          (promise nil))
      (with-lock-held ((%request-coalescer-lock coalescer))
        (let* ((entries (%request-coalescer-entries coalescer))
               (entry (gethash key entries)))
          (if entry
              (progn
                (let ((fingerprint (%coalesced-request-fingerprint entry)))
                  (when (or (and fingerprint
                                 (not (equal fingerprint
                                             idempotency-fingerprint)))
                            (and (null fingerprint)
                               idempotency-fingerprint))
                    (error 'idempotency-conflict
                           :operation operation
                           :message "An in-flight idempotency key has a different fingerprint."
                           :key key
                           :existing-value fingerprint)))
                (setf promise (%coalesced-request-promise entry)))
              (progn
                (setf promise (make-promise)
                      owner-p t)
                (setf (gethash key entries)
                      (%make-coalesced-request promise
                                               idempotency-fingerprint))))))
      (when owner-p
        (let* ((operation-runner
                 (%make-resilience-task-runner
                  thunk
                  :hard-timeout hard-timeout
                  :operation operation
                  :backend :coalescer
                  :clock clock
                  :monotonic-units-per-second
                  monotonic-units-per-second))
               (worker
                 (lambda ()
                  (let ((settled-p nil))
                    (unwind-protect
                         (handler-case
                             (let ((result (funcall operation-runner)))
                               (%remove-coalesced-request coalescer key promise)
                               (deliver promise result)
                               (setf settled-p t))
                           (control-error (condition)
                             (declare (ignore condition)))
                           (promise-already-fulfilled ()
                             (setf settled-p t))
                           (error (condition)
                             (%remove-coalesced-request coalescer key promise)
                             (%deliver-coalesced-error promise condition)
                             (setf settled-p t)))
                      (unless settled-p
                        (%remove-coalesced-request coalescer key promise)
                        (%deliver-coalesced-error
                         promise
                         (make-condition
                          'resilience-error
                          :operation operation
                          :message "The coalesced operation exited without settling its promise.")))
                      (%remove-coalesced-request coalescer key promise))))))
          (handler-case
              (if executor
                  (resilience-executor-submit
                   executor worker
                   :operation operation
                   :clock clock
                   :monotonic-units-per-second monotonic-units-per-second)
                  (future (funcall worker)))
            (error (condition)
              (%remove-coalesced-request coalescer key promise)
              (%deliver-coalesced-error promise condition)))))
      (%await-resilience-promise
       promise timeout
       :operation operation
       :timeout-backend :coalescer-wait))))

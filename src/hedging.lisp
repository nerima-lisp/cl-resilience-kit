(in-package #:cl-resilience-kit)

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

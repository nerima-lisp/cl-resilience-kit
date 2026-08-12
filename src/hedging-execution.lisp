(in-package #:resilience-kit)

(defun %hedging-idempotency-key (idempotency-key)
  (or idempotency-key
      (and (current-resilience-context)
           (resilience-context-idempotency-key
            (current-resilience-context)))))

(defun %ensure-hedging-safe (max-attempts hedge-safe-p idempotency-key operation)
  (when (and (> max-attempts 1)
             (not (or hedge-safe-p
                      (%hedging-idempotency-key idempotency-key))))
    (error 'hedge-unsafe
           :operation operation
           :message "Hedging requires an explicitly safe operation or an idempotency key."
           :reason :non-idempotent-operation)))

(defun %check-hedging-cancellation-token (cancellation-token)
  (when cancellation-token
    (check-cancellation-token cancellation-token)))

(defun %submit-hedge-promise
    (thunk &key executor hard-timeout operation cancellation-token
             clock monotonic-units-per-second)
  (%check-hedging-cancellation-token cancellation-token)
  (%submit-resilience-promise
   thunk
   :executor executor
   :hard-timeout hard-timeout
   :operation operation
   :clock clock
   :monotonic-units-per-second
   monotonic-units-per-second))

(defun %call-single-hedge-attempt
    (thunk &key executor hard-timeout operation clock monotonic-units-per-second)
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
       monotonic-units-per-second)))

(defun %await-primary-hedge-window (promise hedge-after)
  (handler-case
      (values (%await-resilience-promise promise hedge-after) nil t)
    (operation-timed-out (condition)
      (if (%timeout-operation-matches-p condition :await)
          (values nil nil nil)
          (error condition)))
    (resilience-cancelled (condition)
      (error condition))
    (error (condition)
      (values nil condition nil))))

(defun %signal-hedge-exhausted (condition causes operation max-attempts)
  (error 'hedge-exhausted
         :operation operation
         :message "All hedge attempts failed."
         :causes (append (nreverse causes)
                         (promise-all-failed-causes condition))
         :attempts max-attempts))

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
  (%ensure-hedging-safe max-attempts hedge-safe-p idempotency-key operation)
  (%check-hedging-cancellation-token cancellation-token)
  (when (= max-attempts 1)
    (return-from call-with-hedging
      (%call-single-hedge-attempt
       thunk
       :executor executor
       :hard-timeout hard-timeout
       :operation operation
       :clock clock
       :monotonic-units-per-second monotonic-units-per-second)))
  (let ((promises nil)
        (causes nil))
    (labels ((launch ()
               (push (%submit-hedge-promise
                      thunk
                      :executor executor
                      :hard-timeout hard-timeout
                      :operation operation
                      :cancellation-token cancellation-token
                      :clock clock
                      :monotonic-units-per-second
                      monotonic-units-per-second)
                     promises)))
      (launch)
      (unless (zerop hedge-after)
        (multiple-value-bind (result condition completedp)
            (%await-primary-hedge-window (first promises) hedge-after)
          (when completedp
            (return-from call-with-hedging result))
          (when condition
            (push condition causes))))
      (loop repeat (1- max-attempts) do (launch))
      (handler-case
          (%await-resilience-promise
           (promise-any (nreverse promises))
           nil)
        (promise-all-failed (condition)
          (%signal-hedge-exhausted
           condition causes operation max-attempts))))))

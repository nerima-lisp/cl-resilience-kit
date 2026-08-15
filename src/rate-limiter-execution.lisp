(in-package #:resilience-kit)

(defun %rate-limiter-refill! (limiter now capacity refill-rate)
  (let* ((last (%rate-limiter-last-refill limiter))
         (elapsed (max 0d0 (- now last))))
    (when (plusp elapsed)
      (setf (%rate-limiter-tokens limiter)
            (min capacity
                 (+ (%rate-limiter-tokens limiter)
                    (* elapsed refill-rate)))
            (%rate-limiter-last-refill limiter) now))))

(defun %rate-limiter-try-acquire*
    (limiter requested lock clock units capacity refill-rate)
  (with-lock-held (lock)
    (let ((now (%monotonic-seconds clock units)))
      (%rate-limiter-refill! limiter now capacity refill-rate)
      (let ((available (%rate-limiter-tokens limiter)))
        (if (>= available requested)
            (let ((remaining (- available requested)))
              (setf (%rate-limiter-tokens limiter) remaining)
              (values t 0d0 remaining now))
            (let ((shortfall (- requested available)))
              (values nil
                      (and (plusp refill-rate)
                           (/ shortfall refill-rate))
                      available
                      now)))))))

(defun %reject-rate-limiter-acquire
    (requested available retry-after operation reason signal-on-reject-p
     active-handler clock units &optional timestamp)
  (let ((condition (%make-rate-limit-condition
                    requested available retry-after operation)))
    (%emit-resilience-event*
     active-handler :rate-limit-rejected
     operation nil :rate-limit condition nil nil reason
     timestamp nil nil nil
     clock units)
    (if signal-on-reject-p
        (error condition)
        (values nil retry-after))))

(defun rate-limiter-acquire
    (limiter &key (tokens 1d0) wait-p max-wait
             (signal-on-reject-p nil) operation
             cancellation-token event-handler)
  "Acquire TOKENS from LIMITER.

With WAIT-P NIL, return `(VALUES NIL RETRY-AFTER)` when the bucket cannot
admit the request.  With SIGNAL-ON-REJECT-P true, signal
RATE-LIMIT-EXCEEDED instead.  WAIT-P uses the injected sleeper and stops when
MAX-WAIT or the active cooperative deadline would be exceeded.  A fake sleeper
must advance the injected clock if a test wants to observe successful waiting."
  (check-type limiter rate-limiter)
  (unless (and (%finite-real-p tokens) (plusp tokens))
    (error "TOKENS must be a positive real, got ~S." tokens))
  (when cancellation-token
    (check-type cancellation-token cancellation-token))
  (let ((requested (float tokens 1d0))
        (active-token (%active-cancellation-token cancellation-token))
        (active-handler (%active-event-handler event-handler))
        (active-deadline *resilience-deadline*)
        (resilience-clock *resilience-clock*)
        (resilience-units *resilience-monotonic-units-per-second*))
    (when active-token
      (%check-cancellation-token active-token))
    (let ((capacity (%rate-limiter-capacity limiter))
          (clock (%rate-limiter-clock limiter))
          (units (%rate-limiter-monotonic-units-per-second limiter))
          (refill-rate (%rate-limiter-refill-rate limiter))
          (sleeper (%rate-limiter-sleeper limiter))
          (lock (%rate-limiter-lock limiter)))
      (when (and max-wait
                 (or (not (%finite-real-p max-wait)) (minusp max-wait)))
        (error "MAX-WAIT must be NIL or a non-negative real, got ~S." max-wait))
      (when (and active-deadline
                 (>= (%monotonic-seconds clock units) active-deadline))
        (%signal-deadline-exceeded
         resilience-clock
         resilience-units
         active-deadline
         :operation operation
         :stage :rate-limit))
      (when (> requested capacity)
        (return-from rate-limiter-acquire
                     (%reject-rate-limiter-acquire
                      requested
                      (with-lock-held (lock)
                                      (%rate-limiter-tokens limiter))
                      nil operation :request-exceeds-capacity signal-on-reject-p
                      active-handler clock units)))
      (when (and (not wait-p) (null active-token))
        (multiple-value-bind (acquired-p retry-after available observed-at)
            (%rate-limiter-try-acquire*
             limiter requested lock clock units capacity refill-rate)
          (when acquired-p
            (%emit-resilience-event*
             active-handler :rate-limit-acquired
             operation nil nil nil t nil nil
             observed-at nil nil nil
             clock units)
            (return-from rate-limiter-acquire (values t 0d0)))
          (return-from rate-limiter-acquire
                       (%reject-rate-limiter-acquire
                        requested available retry-after operation :insufficient-tokens
                        signal-on-reject-p active-handler clock units observed-at))))
      (let ((wait-start nil))
        (loop
         (when active-token
           (%check-cancellation-token active-token))
         (multiple-value-bind (acquired-p retry-after available observed-at)
             (%rate-limiter-try-acquire*
              limiter requested lock clock units capacity refill-rate)
           (when acquired-p
             (%emit-resilience-event*
              active-handler :rate-limit-acquired
              operation nil nil nil t nil nil
              observed-at nil nil nil
              clock units)
             (return (values t 0d0)))
           (unless wait-p
             (return
              (%reject-rate-limiter-acquire
               requested available retry-after operation :insufficient-tokens
               signal-on-reject-p active-handler clock units observed-at)))
           (let* ((now observed-at)
                  (start (or wait-start (setf wait-start now)))
                  (elapsed (- now start))
                  (deadline-left
                   (and active-deadline
                        (%deadline-remaining-at
                         (if (and (eq resilience-clock clock)
                                  (eql resilience-units units))
                             now
                             (%monotonic-seconds
                              resilience-clock resilience-units))
                         active-deadline))))
             (when (and deadline-left
                        (or (null retry-after)
                            (>= retry-after deadline-left)))
               (%signal-deadline-exceeded
                resilience-clock
                resilience-units
                active-deadline
                :operation operation
                :stage :rate-limit))
             (when (or (null retry-after)
                       (and max-wait (>= elapsed max-wait))
                       (and max-wait (> retry-after (- max-wait elapsed))))
               (return
                (%reject-rate-limiter-acquire
                 requested available retry-after operation :wait-limit
                 signal-on-reject-p active-handler clock units observed-at)))
             (when active-token
               (%check-cancellation-token active-token))
             (%sleep sleeper retry-after)
             (when active-token
               (%check-cancellation-token active-token))
             (let ((after (%monotonic-seconds clock units)))
               (when (and active-deadline
                          (>= after active-deadline))
                 (%signal-deadline-exceeded
                  resilience-clock
                  resilience-units
                  active-deadline
                  :operation operation
                  :stage :rate-limit))
               (when (<= after now)
                 (return
                  (%reject-rate-limiter-acquire
                   requested available retry-after operation :no-clock-progress
                   signal-on-reject-p active-handler clock units observed-at)))))))))))

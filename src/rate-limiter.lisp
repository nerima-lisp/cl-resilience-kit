(in-package #:cl-resilience-kit)

(defclass rate-limiter ()
  ((capacity
    :initarg :capacity
    :reader rate-limiter-capacity)
   (refill-rate
    :initarg :refill-rate
    :reader rate-limiter-refill-rate)
   (clock
    :initarg :clock
    :reader rate-limiter-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader rate-limiter-monotonic-units-per-second)
   (sleeper
    :initarg :sleeper
    :reader rate-limiter-sleeper)
   (%tokens
    :initarg :tokens
    :accessor %rate-limiter-tokens)
   (%last-refill
    :initarg :last-refill
    :accessor %rate-limiter-last-refill)
   (%lock
    :initarg :lock
    :reader %rate-limiter-lock)))

(defun make-rate-limiter
    (&key (capacity 1)
          (refill-rate 1)
          (initial-tokens nil initial-tokens-p)
          (clock nil)
          (monotonic-units-per-second nil)
          (sleeper nil))
  "Create a token bucket using an injected monotonic clock and sleeper.

REFILL-RATE is tokens per normalized second.  ACQUIRE is non-blocking unless
WAIT-P is requested.  Waiting runs in the caller's thread and never creates a
worker, timer, or process-global limiter."
  (unless (and (%finite-real-p capacity) (plusp capacity))
    (error "CAPACITY must be a positive real, got ~S." capacity))
  (unless (and (%finite-real-p refill-rate) (>= refill-rate 0))
    (error "REFILL-RATE must be a non-negative real, got ~S." refill-rate))
  (let* ((active-clock (%active-clock clock))
         (units (%active-monotonic-units-per-second
                 monotonic-units-per-second))
         (initial (if initial-tokens-p initial-tokens capacity)))
    (unless (and (%finite-real-p initial)
                 (>= initial 0)
                 (<= initial capacity))
      (error "INITIAL-TOKENS must be between zero and CAPACITY, got ~S."
             initial))
    (make-instance 'rate-limiter
                   :capacity (float capacity 1d0)
                   :refill-rate (float refill-rate 1d0)
                   :clock active-clock
                   :monotonic-units-per-second units
                   :sleeper (%active-sleeper sleeper)
                   :tokens (float initial 1d0)
                   :last-refill (%monotonic-seconds active-clock units)
                   :lock (cl-concurrent-kit:make-lock
                          :name "cl-resilience-kit.rate-limiter"))))

(defun rate-limiter-tokens (limiter)
  (check-type limiter rate-limiter)
  (cl-concurrent-kit:with-lock-held ((%rate-limiter-lock limiter))
    (%rate-limiter-tokens limiter)))

(defun rate-limiter-last-refill (limiter)
  (check-type limiter rate-limiter)
  (cl-concurrent-kit:with-lock-held ((%rate-limiter-lock limiter))
    (%rate-limiter-last-refill limiter)))

(defun %rate-limiter-refill! (limiter now)
  (let* ((last (%rate-limiter-last-refill limiter))
         (elapsed (max 0d0 (- now last))))
    (when (plusp elapsed)
      (setf (%rate-limiter-tokens limiter)
            (min (rate-limiter-capacity limiter)
                 (+ (%rate-limiter-tokens limiter)
                    (* elapsed (rate-limiter-refill-rate limiter))))
            (%rate-limiter-last-refill limiter) now))))

(defun %rate-limiter-try-acquire (limiter requested)
  (cl-concurrent-kit:with-lock-held ((%rate-limiter-lock limiter))
    (let ((now (%monotonic-seconds
                (rate-limiter-clock limiter)
                (rate-limiter-monotonic-units-per-second limiter))))
      (%rate-limiter-refill! limiter now)
      (if (>= (%rate-limiter-tokens limiter) requested)
          (progn
            (decf (%rate-limiter-tokens limiter) requested)
            (values t 0d0 (%rate-limiter-tokens limiter)))
          (let ((shortfall (- requested (%rate-limiter-tokens limiter))))
            (values nil
                    (and (plusp (rate-limiter-refill-rate limiter))
                         (/ shortfall (rate-limiter-refill-rate limiter)))
                    (%rate-limiter-tokens limiter)))))))

(defun %make-rate-limit-condition
    (requested available retry-after operation)
  (make-condition
   'rate-limit-exceeded
   :message "The rate limiter has insufficient tokens."
   :operation operation
   :requested-tokens requested
   :available-tokens available
   :retry-after retry-after))

(defun %active-deadline-remaining ()
  (and *resilience-deadline*
       (deadline-remaining
        :clock *resilience-clock*
        :monotonic-units-per-second
        *resilience-monotonic-units-per-second*)))

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
        (active-handler (%active-event-handler event-handler)))
    (when active-token
      (check-cancellation-token active-token))
    (labels ((check-token ()
               (when active-token
                 (check-cancellation-token active-token)))
             (reject (available retry-after reason)
               (let ((condition (%make-rate-limit-condition
                                 requested available retry-after operation)))
                 (%emit-resilience-event
                  active-handler :rate-limit-rejected
                  :operation operation
                  :stage :rate-limit
                  :condition condition
                  :reason reason
                  :clock (rate-limiter-clock limiter)
                  :monotonic-units-per-second
                  (rate-limiter-monotonic-units-per-second limiter))
                 (if signal-on-reject-p
                     (error condition)
                     (values nil retry-after)))))
      (when (and max-wait
                 (or (not (%finite-real-p max-wait)) (minusp max-wait)))
        (error "MAX-WAIT must be NIL or a non-negative real, got ~S." max-wait))
      (when (and *resilience-deadline* (deadline-exceeded-p))
        (%signal-deadline-exceeded
         *resilience-clock*
         *resilience-monotonic-units-per-second*
         *resilience-deadline*
         :operation operation
         :stage :rate-limit))
      (when (> requested (rate-limiter-capacity limiter))
        (return-from rate-limiter-acquire
          (reject (rate-limiter-tokens limiter)
                  nil
                  :request-exceeds-capacity)))
      (let ((wait-start nil))
        (loop
          (check-token)
          (multiple-value-bind (acquired-p retry-after available)
              (%rate-limiter-try-acquire limiter requested)
            (when acquired-p
              (%emit-resilience-event
               active-handler :rate-limit-acquired
               :operation operation :result t
               :clock (rate-limiter-clock limiter)
               :monotonic-units-per-second
               (rate-limiter-monotonic-units-per-second limiter))
              (return (values t 0d0)))
            (unless wait-p
              (return (reject available retry-after :insufficient-tokens)))
            (let* ((now (%monotonic-seconds
                         (rate-limiter-clock limiter)
                         (rate-limiter-monotonic-units-per-second limiter)))
                   (start (or wait-start (setf wait-start now)))
                   (elapsed (- now start))
                   (deadline-left (%active-deadline-remaining)))
              (when (and deadline-left
                         (or (null retry-after)
                             (>= retry-after deadline-left)))
                (%signal-deadline-exceeded
                 *resilience-clock*
                 *resilience-monotonic-units-per-second*
                 *resilience-deadline*
                 :operation operation
                 :stage :rate-limit))
              (when (or (null retry-after)
                        (and max-wait (>= elapsed max-wait))
                        (and max-wait (> retry-after (- max-wait elapsed))))
                (return (reject available retry-after :wait-limit)))
              (check-token)
              (%sleep (rate-limiter-sleeper limiter) retry-after)
              (check-token)
              ;; An injected test sleeper may only record the request.  Refuse
              ;; to spin forever when neither the clock nor the bucket advanced.
              (let ((after (%monotonic-seconds
                            (rate-limiter-clock limiter)
                            (rate-limiter-monotonic-units-per-second limiter))))
                (when (and *resilience-deadline*
                           (>= after *resilience-deadline*))
                  (%signal-deadline-exceeded
                   *resilience-clock*
                   *resilience-monotonic-units-per-second*
                   *resilience-deadline*
                   :operation operation
                   :stage :rate-limit))
                (when (<= after now)
                  (return (reject available retry-after :no-clock-progress)))))))))))

(defmacro with-rate-limiter
    ((limiter &key (tokens 1d0) wait-p max-wait
                      (signal-on-reject-p nil) operation
                      cancellation-token event-handler)
     &body body)
  "Acquire tokens before evaluating BODY."
  `(when (rate-limiter-acquire ,limiter
                               :tokens ,tokens
                               :wait-p ,wait-p
                               :max-wait ,max-wait
                               :signal-on-reject-p ,signal-on-reject-p
                               :operation ,operation
                               :cancellation-token ,cancellation-token
                               :event-handler ,event-handler)
     (progn ,@body)))

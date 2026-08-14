(in-package #:resilience-kit)

(defclass rate-limiter ()
  ((capacity
    :initarg :capacity
    :reader rate-limiter-capacity
    :reader %rate-limiter-capacity)
   (refill-rate
    :initarg :refill-rate
    :reader rate-limiter-refill-rate
    :reader %rate-limiter-refill-rate)
   (clock
    :initarg :clock
    :reader rate-limiter-clock
    :reader %rate-limiter-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader rate-limiter-monotonic-units-per-second
    :reader %rate-limiter-monotonic-units-per-second)
   (sleeper
    :initarg :sleeper
    :reader rate-limiter-sleeper
    :reader %rate-limiter-sleeper)
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
                   :lock (make-lock
                          :name "cl-resilience-kit.rate-limiter"))))

(defun rate-limiter-tokens (limiter)
  (check-type limiter rate-limiter)
  (with-lock-held ((%rate-limiter-lock limiter))
    (%rate-limiter-tokens limiter)))

(defun rate-limiter-last-refill (limiter)
  (check-type limiter rate-limiter)
  (with-lock-held ((%rate-limiter-lock limiter))
    (%rate-limiter-last-refill limiter)))

(defun %make-rate-limit-condition
    (requested available retry-after operation)
  (make-condition
   'rate-limit-exceeded
   :message "The rate limiter has insufficient tokens."
   :operation operation
   :requested-tokens requested
   :available-tokens available
   :retry-after retry-after))

(in-package #:resilience-kit)

(defclass retry-budget ()
  ((limit
    :initarg :limit
    :reader retry-budget-limit
    :reader %retry-budget-limit)
   (window
    :initarg :window
    :reader retry-budget-window
    :reader %retry-budget-window)
   (clock
    :initarg :clock
    :reader %retry-budget-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader %retry-budget-monotonic-units-per-second)
   (%used
    :initform 0
    :accessor %retry-budget-used)
   (%window-start
    :initarg :window-start
    :accessor %retry-budget-window-start)
   (%lock
    :initarg :lock
    :reader %retry-budget-lock)))

(defun make-retry-budget
    (&key limit window clock monotonic-units-per-second)
  "Create a lock-protected monotonic fixed-window retry budget.

LIMIT is the maximum number of authorized retries in each WINDOW.  The
initial attempt does not consume a token; RETRY-BUDGET-ACQUIRE consumes one
only when it returns true.  The optional CLOCK and unit scale are captured at
construction time so tests and applications can inject a monotonic source."
  (%make-retry-budget/impl
   :limit limit
   :window window
   :clock clock
   :monotonic-units-per-second monotonic-units-per-second))

(setf (fdefinition '%make-retry-budget/impl)
      (lambda (&key limit window clock monotonic-units-per-second)
        (unless (and (integerp limit) (>= limit 1))
          (error "LIMIT must be a positive integer, got ~S." limit))
        (unless (and (%finite-real-p window) (plusp window))
          (error "WINDOW must be a positive real, got ~S." window))
        (let* ((active-clock (%active-clock clock))
               (units (%active-monotonic-units-per-second
                       monotonic-units-per-second))
               (constructor #'make-instance))
          (funcall constructor
                   'retry-budget
                   :limit limit
                   :window (float window 1d0)
                   :clock active-clock
                   :monotonic-units-per-second units
                   :window-start (%monotonic-seconds active-clock units)
                   :lock (cl-concurrent-kit:make-lock
                          :name "cl-resilience-kit.retry-budget")))))

(defclass distributed-retry-budget (retry-budget)
  ((store
    :initarg :store
    :reader distributed-retry-budget-store
    :reader %distributed-retry-budget-store)
   (key
    :initarg :key
    :reader distributed-retry-budget-key
    :reader %distributed-retry-budget-key)))

(defun make-distributed-retry-budget
    (&key store key limit window clock monotonic-units-per-second)
  "Create a retry budget backed by a compare-and-set STATE-STORE.

The store value is an implementation detail consisting of a window start and
the consumed token count.  Every successful acquisition is committed with a
compare-and-set, so independent processes sharing STORE and KEY observe one
budget.  The store must provide atomic versioned writes; a plain key/value
store is not sufficient for this contract."
  (%make-distributed-retry-budget/impl
   :store store
   :key key
   :limit limit
   :window window
   :clock clock
   :monotonic-units-per-second monotonic-units-per-second))

(setf (fdefinition '%make-distributed-retry-budget/impl)
      (lambda (&key store key limit window clock monotonic-units-per-second)
        (check-type store resilience-state-store)
        (check-type key string)
        (unless (and (integerp limit) (>= limit 1))
          (error "LIMIT must be a positive integer, got ~S." limit))
        (unless (and (%finite-real-p window) (plusp window))
          (error "WINDOW must be a positive real, got ~S." window))
        (let* ((active-clock (%active-clock clock))
               (units (%active-monotonic-units-per-second
                       monotonic-units-per-second))
               (constructor #'make-instance))
          (funcall constructor
                   'distributed-retry-budget
                   :limit limit
                   :window (float window 1d0)
                   :clock active-clock
                   :monotonic-units-per-second units
                   :window-start (%monotonic-seconds active-clock units)
                   :lock (cl-concurrent-kit:make-lock
                          :name "cl-resilience-kit.distributed-retry-budget")
                   :store store
                   :key key))))

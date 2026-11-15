(in-package #:resilience-kit/test)

(defun make-fake-clock (&key (start 0) (monotonic-start start))
  (let ((now monotonic-start))
    (lambda (&optional (delta 0) (monotonic-delta delta))
      (incf now monotonic-delta))))

(defun advance-fake-clock (clock delta &key (monotonic-delta delta))
  (funcall clock delta monotonic-delta)
  clock)

(defun make-sleeper (&key (sleep-fn #'sleep))
  sleep-fn)

(defun make-test-random-source (&key values)
  (let ((remaining (copy-list values)))
    (lambda (limit)
      (unless remaining
        (error "Test random source has no remaining values for limit ~S" limit))
      (let ((value (first remaining)))
        (unless (and (realp value) (<= 0 value) (< value limit)
                     (or (not (integerp limit)) (integerp value)))
          (error "Test random source value ~S is outside limit ~S" value limit))
        (pop remaining)))))

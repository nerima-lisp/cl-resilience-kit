(in-package #:resilience-kit)

(defun %finite-real-p (value)
  "Return true when VALUE is a finite real representable as a double float."
  (and (realp value)
       (= value value)
       (<= (abs value) most-positive-double-float)))

(defun %finite-duration-seconds (duration)
  (when (and (%finite-real-p duration)
             (not (minusp duration)))
    (handler-case
        (float duration 1d0)
      (type-error ()
        nil)
      (floating-point-overflow ()
        nil))))

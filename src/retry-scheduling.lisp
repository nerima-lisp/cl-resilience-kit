(in-package #:resilience-kit)

(defun %capped-delay (policy delay)
  (let ((non-negative (%ensure-non-negative-real delay "RETRY-DELAY")))
    (if (retry-policy-max-delay policy)
        (min (retry-policy-max-delay policy) non-negative)
        non-negative)))

(defun %bounded-product (left right)
  (let ((left (float left 1d0))
        (right (float right 1d0)))
    (if (or (zerop left) (zerop right)
            (<= left (/ most-positive-double-float right)))
        (* left right)
        most-positive-double-float)))

(defun %bounded-exponential-delay (policy retry-number)
  (let* ((limit (or (retry-policy-max-delay policy)
                    most-positive-double-float))
         (value (min limit (retry-policy-initial-delay policy)))
         (multiplier (retry-policy-multiplier policy)))
    (cond ((zerop value) 0d0)
          ((= multiplier 1d0) value)
          (t
           ;; Exponentiation by squaring keeps a very large retry number from
           ;; turning delay calculation into an unbounded linear loop.
           (let ((exponent (1- retry-number))
                 (factor multiplier))
             (loop while (plusp exponent)
                   do (when (oddp exponent)
                        (setf value
                              (min limit (%bounded-product value factor))))
                      (when (or (= value limit)
                                (= exponent 1))
                        (return))
                      (setf exponent (floor exponent 2)
                            factor
                            (min limit (%bounded-product factor factor))))
             value)))))

(defun %random-below (random-source upper-bound)
  (if (<= upper-bound 0d0)
      0d0
      (* upper-bound (%random-unit random-source))))

(defun compute-backoff-delay (policy retry-number &key previous-delay)
  "Compute the delay before RETRY-NUMBER, whose first value is one.

The exponential base is capped by MAX-DELAY before jitter and the final
result is capped again.  Jitter is one of :NONE, :FULL, :EQUAL, or
:DECORRELATED and consumes only the injected boundary random source."
  (check-type policy retry-policy)
  (%ensure-retry-policy-option
   (and (integerp retry-number) (>= retry-number 1))
   "RETRY-NUMBER must be a positive integer, got ~S."
   retry-number)
  (when (and previous-delay
             (or (not (%finite-real-p previous-delay))
                 (minusp previous-delay)))
    (error "PREVIOUS-DELAY must be NIL or a non-negative real, got ~S."
           previous-delay))
  (let* ((base (%bounded-exponential-delay policy retry-number))
         (jitter (retry-policy-jitter policy))
         (random-source (retry-policy-random-source policy)))
    (%capped-delay
     policy
     (case jitter
       (:none base)
       (:full (%random-below random-source base))
       (:equal (+ (/ base 2d0)
                  (%random-below random-source (/ base 2d0))))
       (:decorrelated
       (let* ((lower (retry-policy-initial-delay policy))
               (previous (or previous-delay lower))
               (upper (max lower (%bounded-product 3d0 previous)))
               (upper (if (retry-policy-max-delay policy)
                          (min upper (retry-policy-max-delay policy))
                          upper)))
          (if (<= upper lower)
              lower
              (+ lower (%random-below random-source (- upper lower))))))
       (otherwise
        (error "Unknown jitter strategy ~S." jitter))))))

(defun %delay-with-hint (policy attempt previous-delay decision)
  (let ((computed (compute-backoff-delay policy attempt
                                         :previous-delay previous-delay))
        (hint (retry-decision-delay-hint decision)))
    (when hint
      (%ensure-retry-policy-option
       (and (%finite-real-p hint) (not (minusp hint)))
       "A retry delay hint must be a non-negative real, got ~S."
       hint))
    (%capped-delay policy (if hint (max computed hint) computed))))

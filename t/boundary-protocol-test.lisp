(in-package #:resilience-kit/test)

(describe "callable effect protocols"
  (it "uses internal-time units for the default clock"
    (let ((before (get-internal-real-time))
          (observed (resilience-kit::%now)))
      (expect (<= (/ (float before 1d0)
                     (float internal-time-units-per-second 1d0))
                  observed
                  (/ (float (get-internal-real-time) 1d0)
                     (float internal-time-units-per-second 1d0)))
              :to-be t)))

  (it "uses CL:SLEEP for the default sleeper"
    (expect (resilience-kit::%active-sleeper nil) :to-be #'sleep))

  (it "normalizes an injected clock and invokes an injected sleeper"
    (let ((now 20)
          (slept nil))
      (let ((limiter (make-rate-limiter
                      :capacity 1 :initial-tokens 0 :refill-rate 1
                      :clock (lambda () now)
                      :monotonic-units-per-second 10
                      :sleeper (lambda (seconds)
                                 (push seconds slept)
                                 (incf now (* seconds 10))))))
        (expect (rate-limiter-last-refill limiter) :to-be 2d0)
        (expect (rate-limiter-acquire limiter :wait-p t) :to-be t)
        (expect slept :to-equal '(1d0))
        (expect (rate-limiter-tokens limiter) :to-be 0d0))))

  (it "uses the injected random callback for jitter with its integer limit"
    (let ((limits nil))
      (let ((policy (make-retry-policy
                     :initial-delay 4 :jitter :full
                     :random-source (lambda (limit)
                                      (push limit limits)
                                      250000))))
        (expect (compute-backoff-delay policy 1) :to-be 1d0)
        (expect limits :to-equal '(1000000)))))

  (it "keeps default random state private to each policy"
    (let* ((state (make-random-state nil))
           (*random-state* (make-random-state state))
           (first (make-retry-policy :initial-delay 1 :jitter :full))
           (second (make-retry-policy :initial-delay 1 :jitter :full)))
      (expect (eq (resilience-kit:retry-policy-random-source first)
                  (resilience-kit:retry-policy-random-source second))
              :to-be nil)
      (loop repeat 10
            do (let ((delay (compute-backoff-delay first 1)))
                 (expect (<= 0d0 delay) :to-be t)
                 (expect (< delay 1d0) :to-be t)))
      (expect (random 1000000 *random-state*)
              :to-be (random 1000000 state))))

  (it "advances injected time and consumes deterministic samples without fallback"
    (let* ((now 7)
           (samples '(0 999999))
           (clock (lambda () now))
           (policy (make-retry-policy
                    :initial-delay 1 :jitter :full
                    :random-source (lambda (limit)
                                     (declare (ignore limit))
                                     (unless samples
                                       (error "The random samples are exhausted."))
                                     (pop samples)))))
      (expect (funcall clock) :to-be 7)
      (incf now 2)
      (expect (funcall clock) :to-be 9)
      (expect (compute-backoff-delay policy 1) :to-be 0d0)
      (expect (compute-backoff-delay policy 1) :to-be 0.999999d0)
      (expect-condition-form (compute-backoff-delay policy 1) 'error)))

  (it "rejects non-function effect objects and function-name symbols"
    (expect-error-forms
      (make-rate-limiter :clock (list :old-clock))
      (make-rate-limiter :clock 'get-internal-real-time)
      (make-rate-limiter :sleeper (list :old-sleeper))
      (make-rate-limiter :sleeper 'sleep)
      (make-retry-policy :random-source (list :old-random-source))
      (make-retry-policy :random-source 'random)))

  (it "names invalid clock and sleeper options in validation errors"
    (let ((clock-error (expect-condition-form
                       (make-rate-limiter :clock 'get-internal-real-time) 'error))
          (sleeper-error (expect-condition-form
                         (make-rate-limiter :sleeper 'sleep) 'error)))
      (expect (princ-to-string clock-error)
              :to-contain "CLOCK must be a function or NIL")
      (expect (princ-to-string sleeper-error)
              :to-contain "SLEEPER must be a function or NIL")))

  (it "validates the sleeper before the single-attempt retry fast path"
    (let ((calls 0))
      (let ((condition
              (expect-condition-form
               (call-with-retry (make-retry-policy :max-attempts 1)
                                (lambda () (incf calls))
                                :sleeper 'sleep)
               'error)))
        (expect calls :to-be 0)
        (expect (princ-to-string condition)
                :to-contain "SLEEPER must be a function or NIL")))))

(in-package #:resilience-kit/test)

(describe "retry deadline and result boundaries"
  (it "uses the earliest of overall and per-attempt deadlines"
    (let* ((fixture (make-test-fixture))
           (policy (make-retry-policy :max-attempts 1))
           (remaining
             (call-with-retry
              policy
              #'deadline-remaining
              :overall-timeout 10d0
              :per-attempt-timeout 1d0
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+
              :sleeper (test-fixture-sleeper fixture))))
      (expect (approximately-equal-p remaining 1d0)
              :to-be-truthy)))

  (it "rejects simultaneous overall timeout and deadline inputs"
    (let ((policy (make-retry-policy :max-attempts 1)))
      (expect-condition
       (lambda ()
         (call-with-retry
          policy
          (lambda () :never)
          :overall-timeout 1d0
          :overall-deadline 1d0))
       'error)))

  (it "rejects a non-finite overall deadline"
    (let ((policy (make-retry-policy :max-attempts 1)))
      (expect-condition
       (lambda ()
         (call-with-retry
          policy
          (lambda () :never)
          :overall-deadline :invalid))
       'error)))

  (it "rejects an attempt before invoking the operation when its timeout is zero"
    (let* ((fixture (make-test-fixture))
           (calls 0)
           (policy (make-retry-policy
                    :max-attempts
                    1
                    :retry-safe-p
                    t))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda ()
                         (incf calls)
                         :never)
                       :per-attempt-timeout
                       0d0
                       :clock
                       (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper
                       (test-fixture-sleeper fixture)))
                    'attempt-timeout)))
      (expect calls :to-be 0)
      (expect (deadline-exceeded-stage caught)
              :to-be
              :before-attempt)
      (expect (attempt-timeout-timeout caught)
              :to-be
              0d0)))

  (it "reports the last result when result classification exhausts retries"
    (let* ((fixture (make-test-fixture))
           (policy (make-retry-policy
                    :max-attempts
                    1
                    :retry-safe-p
                    t
                    :result-classifier
                    (lambda (result attempt)
                      (declare (ignore result attempt))
                      t)))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda () :retry)
                       :clock
                       (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper
                       (test-fixture-sleeper fixture)))
                    'retry-exhausted)))
      (expect (retry-exhausted-attempts caught) :to-be 1)
      (expect (retry-exhausted-last-result caught)
              :to-be
              :retry)))

  (it "rejects an already expired overall deadline before invocation"
    (let* ((fixture (make-test-fixture))
           (calls 0)
           (policy (make-retry-policy :max-attempts 1))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda ()
                         (incf calls)
                         :never)
                       :overall-deadline
                       -1d0
                       :clock
                       (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper
                       (test-fixture-sleeper fixture)))
                    'deadline-exceeded)))
      (expect calls :to-be 0)
      (expect (deadline-exceeded-stage caught)
              :to-be
              :before-operation)))

  (it "rejects invalid backoff inputs and delay hints"
    (let* ((fixture (make-test-fixture))
           (policy (make-retry-policy
                    :max-attempts 2
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (values t -1)))))
      (expect-condition
       (lambda () (compute-backoff-delay policy 0))
       'error)
      (expect-condition
       (lambda () (compute-backoff-delay policy 1 :previous-delay -1))
       'error)
      (expect-condition
       (lambda ()
         (call-with-retry
          policy
          (lambda () (error "transient"))
          :clock (test-fixture-clock fixture)
          :monotonic-units-per-second
          +test-monotonic-units-per-second+
          :sleeper (test-fixture-sleeper fixture)))
       'error))))

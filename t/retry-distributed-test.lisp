(in-package #:cl-resilience-kit/test)
(describe "distributed state and budgets"
  (it "provides versioned state-store compare-and-set operations"
    (let ((store (cl-resilience-kit:make-memory-state-store)))
      (expect
       (cl-resilience-kit:state-store-put-if-version
        store "retry/a" '(:value 1) nil)
       :to-be
       1)
      (multiple-value-bind (value version)
          (cl-resilience-kit:state-store-get store "retry/a")
        (expect value :to-equal '(:value 1))
        (expect version :to-be 1))
      (let ((caught
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:state-store-put-if-version
                  store "retry/a" '(:value 2) nil))
               'cl-resilience-kit:resilience-store-conflict)))
        (expect
         (cl-resilience-kit:resilience-store-conflict-expected-version caught)
         :to-be
         nil)
        (expect
         (cl-resilience-kit:resilience-store-conflict-actual-version caught)
         :to-be
         1))
      (expect
       (cl-resilience-kit:state-store-put-if-version
        store "retry/a" '(:value 2) 1)
       :to-be
       2)
      (expect
       (length (cl-resilience-kit:state-store-scan-prefix store "retry/"))
       :to-be
       1)
      (expect
       (cl-resilience-kit:state-store-delete-if-version store "retry/a" 2)
       :to-be-truthy)
      (multiple-value-bind (value version)
          (cl-resilience-kit:state-store-get store "retry/a")
        (expect value :to-be nil)
        (expect version :to-be nil))))

  (it "shares retry budget state across distributed budget instances"
    (let* ((fixture (make-test-fixture))
           (store (cl-resilience-kit:make-memory-state-store))
           (first-budget
             (cl-resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/shared"
              :limit 2
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (second-budget
             (cl-resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/shared"
              :limit 2
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect (retry-budget-used first-budget) :to-be 0)
      (expect (retry-budget-acquire first-budget) :to-be-truthy)
      (expect (retry-budget-used second-budget) :to-be 1)
      (expect (retry-budget-acquire second-budget) :to-be-truthy)
      (expect (retry-budget-acquire first-budget) :to-be nil)
      (expect (retry-budget-remaining second-budget) :to-be 0)
      (advance-fixture fixture 10)
      (expect (retry-budget-acquire second-budget) :to-be-truthy)
      (expect (retry-budget-used first-budget) :to-be 1))))

(describe "leases, lifecycle, and health boundaries"
  (it "uses fencing tokens and rejects stale lease owners"
    (let* ((fixture (make-test-fixture))
           (store (cl-resilience-kit:make-memory-lease-store
                   :clock (test-fixture-clock fixture)
                   :monotonic-units-per-second
                   +test-monotonic-units-per-second+))
           (first-lease
             (cl-resilience-kit:acquire-resilience-lease
              store "leader" "owner-a" :ttl 2))
           (unavailable
             (expect-condition
              (lambda ()
                (cl-resilience-kit:acquire-resilience-lease
                 store "leader" "owner-b" :ttl 2))
              'cl-resilience-kit:resilience-lease-unavailable)))
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lease-fencing-token first-lease)
              :to-be 1)
      (expect (cl-resilience-kit:resilience-lease-unavailable-key unavailable)
              :to-be
              "leader")
      (expect
       (cl-resilience-kit:renew-resilience-lease first-lease :ttl 3)
       :to-be
       first-lease)
      (advance-fixture fixture 3)
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be nil)
      (let ((second-lease
              (cl-resilience-kit:acquire-resilience-lease
               store "leader" "owner-b" :ttl 2)))
        (expect
         (> (cl-resilience-kit:resilience-lease-fencing-token second-lease)
            (cl-resilience-kit:resilience-lease-fencing-token first-lease))
         :to-be-truthy)
        (expect-condition
         (lambda ()
           (cl-resilience-kit:renew-resilience-lease first-lease))
         'cl-resilience-kit:resilience-lease-lost)
        (expect (cl-resilience-kit:release-resilience-lease second-lease)
                :to-be-truthy)
        (expect (cl-resilience-kit:resilience-lease-held-p second-lease)
                :to-be nil))))

  (it "releases a lease on nonlocal exit"
    (let ((store (cl-resilience-kit:make-memory-lease-store)))
      (expect
       (catch :lease-exit
         (cl-resilience-kit:with-resilience-lease
             (lease store "temporary" "owner")
           (throw :lease-exit :escaped)))
       :to-be
       :escaped)
      (expect
       (cl-resilience-kit:acquire-resilience-lease
        store "temporary" "next-owner")
       :to-be-truthy)))

  (it "drains active lifecycle entries before stopping"
    (let ((lifecycle (cl-resilience-kit:make-resilience-lifecycle)))
      (expect (cl-resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :running)
      (expect
       (cl-resilience-kit:enter-resilience-lifecycle lifecycle
                                                     :operation :read)
       :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lifecycle-active lifecycle)
              :to-be 1)
      (expect (cl-resilience-kit:begin-resilience-drain lifecycle)
              :to-be
              :draining)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:enter-resilience-lifecycle lifecycle
                                                       :operation :late))
       'cl-resilience-kit:resilience-draining)
      (expect (cl-resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be 0)
      (expect (cl-resilience-kit:await-resilience-drained lifecycle)
              :to-be-truthy)
      (expect (cl-resilience-kit:stop-resilience-lifecycle lifecycle)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :stopped)))

  (it "reports readiness from registered health checks"
    (let ((registry (cl-resilience-kit:make-health-registry)))
      (cl-resilience-kit:register-health-check
       registry :database (lambda () t))
      (cl-resilience-kit:register-health-check
       registry :cache (lambda () nil))
      (expect (cl-resilience-kit:health-live-p registry) :to-be-truthy)
      (expect (cl-resilience-kit:health-ready-p registry) :to-be nil)
      (expect
       (every (lambda (entry)
                (member (getf entry :status) '(:healthy :unhealthy)))
              (cl-resilience-kit:health-report registry))
       :to-be-truthy)
      (cl-resilience-kit:unregister-health-check registry :cache)
      (expect (cl-resilience-kit:health-ready-p registry) :to-be-truthy))))

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

(describe "backoff boundary policies"
  (it "caps decorrelated jitter at the configured maximum"
    (let ((policy (make-retry-policy
                   :initial-delay 1d0
                   :max-delay 1d0
                   :jitter :decorrelated)))
      (expect
       (compute-backoff-delay policy 2 :previous-delay 100d0)
       :to-be
       1d0))))

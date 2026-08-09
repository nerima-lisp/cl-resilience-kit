(in-package #:cl-resilience-kit/test)

(describe "local public boundary contracts"
  (it "preserves keyword cancellation reasons and first-write semantics"
    (let ((token (cl-resilience-kit:make-cancellation-token)))
      (expect (cl-resilience-kit:cancel-cancellation-token
               token
               :reason
               :manual)
              :to-be
              token)
      (expect (cl-resilience-kit:cancellation-token-reason token)
              :to-be
              :manual)
      (cl-resilience-kit:cancel-cancellation-token token :later)
      (expect (cl-resilience-kit:cancellation-token-reason token)
              :to-be
              :manual)))

  (it "merges an omitted context operation from its parent"
    (let* ((base (cl-resilience-kit:make-resilience-context
                  :operation
                  :base))
           (overlay (cl-resilience-kit:make-resilience-context
                     :correlation-id
                     "inner"))
           (merged (cl-resilience-kit:merge-resilience-context
                    base
                    overlay)))
      (expect (cl-resilience-kit:resilience-context-operation merged)
              :to-be
              :base)))

  (it "allows an explicitly unbounded deadline call"
    (expect (cl-resilience-kit:call-with-deadline
             (lambda () :unbounded))
            :to-be
            :unbounded))

  (it "rejects nil lease owners and invalid breaker reset timeouts"
    (let ((store (cl-resilience-kit:make-memory-lease-store)))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:acquire-resilience-lease
          store
          "nil-owner"
          nil))
       'error))
    (expect-condition
     (lambda ()
       (cl-resilience-kit:make-circuit-breaker :reset-timeout 0))
     'error))

  (it "uses the timed condition wait path while draining"
    (let ((lifecycle (cl-resilience-kit:make-resilience-lifecycle)))
      (cl-resilience-kit:enter-resilience-lifecycle lifecycle :operation :timed-wait)
      (cl-resilience-kit:begin-resilience-drain lifecycle)
      (expect (cl-resilience-kit:await-resilience-drained
               lifecycle
               :timeout
               0.01d0)
              :to-be
              nil)
      (expect (cl-resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be
              0)
      (expect (cl-resilience-kit:stop-resilience-lifecycle lifecycle)
              :to-be-truthy))))

  (it "coordinates lifecycle draining and health readiness"
    (let ((lifecycle (cl-resilience-kit:make-resilience-lifecycle)))
      (cl-resilience-kit:enter-resilience-lifecycle lifecycle
                                                    :operation :read)
      (expect (cl-resilience-kit:stop-resilience-lifecycle lifecycle
                                                           :timeout 0)
              :to-be
              nil)
      (expect (cl-resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :draining)
      (cl-resilience-kit:leave-resilience-lifecycle lifecycle)
      (expect (cl-resilience-kit:stop-resilience-lifecycle lifecycle)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :stopped)
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:enter-resilience-lifecycle lifecycle))
               'cl-resilience-kit:resilience-draining)))
        (expect condition :to-be-truthy)))
    (let ((registry (cl-resilience-kit:make-health-registry)))
      (cl-resilience-kit:register-health-check
       registry :database (lambda () (error "database unavailable")))
      (let ((report (cl-resilience-kit:health-report registry)))
        (expect report :to-be-truthy)
        (expect (getf (first report) :status) :to-be :unhealthy)
        (expect (getf (first report) :condition) :to-be-truthy))
      (expect (cl-resilience-kit:health-ready-p registry) :to-be nil)
      (expect (cl-resilience-kit:health-live-p registry) :to-be-truthy)
      (expect (cl-resilience-kit:unregister-health-check registry :database)
              :to-be-truthy)
      (expect (cl-resilience-kit:health-report registry) :to-be nil)))

  (it "rejects unsafe hedging and reports all failed attempts"
    (let ((condition
            (expect-condition
             (lambda ()
               (cl-resilience-kit:call-with-hedging
                (lambda () :ok)
                :max-attempts 2))
             'cl-resilience-kit:hedge-unsafe)))
      (expect condition :to-be-truthy))
    (let ((condition
            (expect-condition
             (lambda ()
               (cl-resilience-kit:call-with-hedging
                (lambda () (error "hedged failure"))
                :hedge-safe-p t
                :max-attempts 2
                :hedge-after 0d0))
             'cl-resilience-kit:hedge-exhausted)))
      (expect condition :to-be-truthy)
      (expect (cl-resilience-kit:hedge-exhausted-attempts condition)
              :to-be
              2)
      (expect (length (cl-resilience-kit:hedge-exhausted-causes condition))
              :to-be
              2)))

  (it "supports block macros for execution boundaries"
    (expect
     (cl-resilience-kit:with-hedging
         (:max-attempts 1 :operation :macro-hedge)
       :hedged)
     :to-be
     :hedged)
    (let ((coalescer (cl-resilience-kit:make-request-coalescer)))
      (expect
       (cl-resilience-kit:with-request-coalescing
           (coalescer :key "macro-key" :operation :macro-coalesce)
         :coalesced)
       :to-be
       :coalesced)
      (expect (cl-resilience-kit:request-coalescer-size coalescer)
              :to-be
              0))
    (let ((executor (cl-resilience-kit:make-resilience-executor :size 1)))
      (unwind-protect
           (expect
            (cl-resilience-kit:with-resilience-executor
                (executor :timeout 1d0 :operation :macro-executor)
              :executed)
            :to-be
            :executed)
        (cl-resilience-kit:resilience-executor-shutdown executor :wait t))))

  (it "normalizes classifier retry decisions"
    (let ((policy
            (cl-resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               (cl-resilience-kit:make-retry-decision
                :retry-p t
                :delay-hint 4
                :reason :transient)))))
      (multiple-value-bind (retry-p decision)
          (cl-resilience-kit:retry-policy-should-retry-p
           policy 1 :condition (make-condition 'error))
        (expect retry-p :to-be-truthy)
        (expect (cl-resilience-kit:retry-decision-retry-p decision)
                :to-be-truthy)
        (expect (cl-resilience-kit:retry-decision-delay-hint decision)
                :to-be
                4)
        (expect (cl-resilience-kit:retry-decision-reason decision)
                :to-be
                :transient))))

  (it "validates retry policy inputs and classifier boundaries"
    (dolist (arguments '((:max-attempts 0)
                         (:initial-delay -1)
                         (:multiplier 0)
                         (:max-delay -1)
                         (:jitter :unknown)))
      (expect-condition
       (lambda ()
         (apply #'cl-resilience-kit:make-retry-policy arguments))
       'error))
    (let ((policy (cl-resilience-kit:make-retry-policy)))
      (multiple-value-bind (retry-p decision)
          (cl-resilience-kit:retry-policy-should-retry-p
           policy 1 :condition (make-condition 'error))
        (expect retry-p :to-be nil)
        (expect (cl-resilience-kit:retry-decision-reason decision)
                :to-be
                :not-retry-safe))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:retry-policy-should-retry-p
          policy 0 :result :ok))
       'error)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:retry-policy-should-retry-p policy 1))
       'error))
    (let ((policy
            (cl-resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t)))
      (multiple-value-bind (retry-p decision)
          (cl-resilience-kit:retry-policy-should-retry-p
           policy 1 :result nil)
        (expect retry-p :to-be nil)
        (expect (cl-resilience-kit:retry-decision-reason decision)
                :to-be
                :no-classifier)))
    (let ((policy
            (cl-resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t
             :result-classifier
             (lambda (result attempt)
               (declare (ignore attempt))
               (if (eq result :retryable) 2 nil)))))
      (multiple-value-bind (retry-p decision)
          (cl-resilience-kit:retry-policy-should-retry-p
           policy 1 :result :retryable)
        (expect retry-p :to-be-truthy)
        (expect (cl-resilience-kit:retry-decision-delay-hint decision)
                :to-be
                2))
      (expect (cl-resilience-kit:compute-backoff-delay
               (cl-resilience-kit:make-retry-policy
                :initial-delay 3
                :multiplier 1)
               100)
              :to-be
              3d0)))

  (it "enforces compare-and-set and lease renewal boundaries"
    (let ((store (cl-resilience-kit:make-memory-state-store)))
      (expect (cl-resilience-kit:state-store-put-if-version
               store "state/a" (list :value 1) nil)
              :to-be
              1)
      (expect (cl-resilience-kit:state-store-put-if-version
               store "state/b" (list :value 2) nil)
              :to-be
              1)
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:state-store-put-if-version
                  store "state/a" (list :value 3) nil))
               'cl-resilience-kit:resilience-store-conflict)))
        (expect (cl-resilience-kit:resilience-store-conflict-expected-version
                 condition)
                :to-be
                nil)
        (expect (cl-resilience-kit:resilience-store-conflict-actual-version
                 condition)
                :to-be
                1))
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:state-store-delete-if-version
                  store "state/b" 2))
               'cl-resilience-kit:resilience-store-conflict)))
        (expect (cl-resilience-kit:resilience-store-conflict-actual-version
                 condition)
                :to-be
                1))
      (expect (length
               (cl-resilience-kit:state-store-scan-prefix store "state/") )
              :to-be
              2)
      (expect (cl-resilience-kit:state-store-put-if-version
               store "other/a" (list :value 4) nil)
              :to-be
              1)
      (expect (length
               (cl-resilience-kit:state-store-scan-prefix store "state/") )
              :to-be
              2)
      (expect (cl-resilience-kit:state-store-delete-if-version
               store "state/a" 1)
              :to-be-truthy)
      (multiple-value-bind (value version)
          (cl-resilience-kit:state-store-get store "state/a")
        (expect value :to-be nil)
        (expect version :to-be nil))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:state-store-scan-prefix store 1))
       'type-error))
    (let* ((fixture (make-test-fixture :start 10))
           (store
             (cl-resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (first-lease
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 2)))
      (expect (cl-resilience-kit:renew-resilience-lease
               first-lease :ttl 5)
              :to-be
              first-lease)
      (expect (cl-resilience-kit:resilience-lease-ttl first-lease)
              :to-be
              5d0)
      (expect (approximately-equal-p
               (cl-resilience-kit:resilience-lease-expires-at first-lease)
               15)
              :to-be-truthy)
      (advance-fixture fixture 6)
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be
              nil)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:renew-resilience-lease first-lease))
       'cl-resilience-kit:resilience-lease-lost)
      (let ((second-lease
              (cl-resilience-kit:acquire-resilience-lease
               store "lease/a" "owner-b" :ttl 3)))
        (expect (> (cl-resilience-kit:resilience-lease-fencing-token
                    second-lease)
                   (cl-resilience-kit:resilience-lease-fencing-token
                    first-lease))
                :to-be-truthy)
        (expect (cl-resilience-kit:release-resilience-lease
                 first-lease :ignore-lost-p t)
                :to-be
                nil)
        (expect-condition
         (lambda ()
           (cl-resilience-kit:release-resilience-lease first-lease))
         'cl-resilience-kit:resilience-lease-lost)
        (expect (cl-resilience-kit:release-resilience-lease second-lease)
                :to-be-truthy))
      (let ((inside nil))
        (cl-resilience-kit:with-resilience-lease
            (lease store "lease/macro" "macro-owner" :ttl 2)
          (setf inside
                (cl-resilience-kit:resilience-lease-held-p lease)))
        (expect inside :to-be-truthy))))

  (it "completes local half-open probes and classifier failures"
    (expect-condition
     (lambda ()
       (cl-resilience-kit:make-circuit-breaker :failure-threshold 0))
     'error)
    (expect-condition
     (lambda ()
       (cl-resilience-kit:make-circuit-breaker :condition-classifier 1))
     'error)
    (let* ((fixture (make-test-fixture :start 0))
           (breaker
             (cl-resilience-kit:make-circuit-breaker
              :failure-threshold 1
              :reset-timeout 2
              :half-open-probe-limit 1
              :success-threshold 2
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect (cl-resilience-kit:circuit-breaker-failure-threshold breaker)
              :to-be
              1)
      (expect (approximately-equal-p
               (cl-resilience-kit:circuit-breaker-reset-timeout breaker)
               2)
              :to-be-truthy)
      (expect (cl-resilience-kit:circuit-breaker-half-open-probe-limit breaker)
              :to-be
              1)
      (expect (cl-resilience-kit:circuit-breaker-success-threshold breaker)
              :to-be
              2)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:circuit-breaker-call
          breaker (lambda () (error "local failure"))))
       'error)
      (expect (cl-resilience-kit:circuit-breaker-state breaker)
              :to-be
              :open)
      (advance-fixture fixture 2)
      (expect (cl-resilience-kit:circuit-breaker-call
               breaker (lambda () :probe-one))
              :to-be
              :probe-one)
      (expect (cl-resilience-kit:circuit-breaker-state breaker)
              :to-be
              :half-open)
      (expect (cl-resilience-kit:circuit-breaker-call
               breaker (lambda () :probe-two))
              :to-be
              :probe-two)
      (expect (cl-resilience-kit:circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (cl-resilience-kit:with-circuit-breaker
                   (breaker :operation :macro)
                 :macro-result)
              :to-be
              :macro-result))
    (let ((breaker
            (cl-resilience-kit:make-circuit-breaker
             :failure-threshold 1
             :result-classifier
             (lambda (value attempt)
               (declare (ignore value attempt))
               (error "result classifier failure")))))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:circuit-breaker-call
          breaker (lambda () :ok)))
       'error)
      (expect (cl-resilience-kit:circuit-breaker-state breaker)
              :to-be
              :open))
    (let ((breaker
            (cl-resilience-kit:make-circuit-breaker
             :failure-threshold 1
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               nil))))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:circuit-breaker-call
          breaker (lambda () (error "ignored local failure"))))
       'error)
      (expect (cl-resilience-kit:circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (cl-resilience-kit:circuit-breaker-failure-count breaker)
              :to-be
              0)))

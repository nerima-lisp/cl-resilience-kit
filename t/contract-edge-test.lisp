(in-package #:resilience-kit/test)

(describe "local public boundary contracts"
  (it "preserves keyword cancellation reasons and first-write semantics"
    (let ((token (resilience-kit:make-cancellation-token)))
      (expect (resilience-kit:cancel-cancellation-token
               token
               :reason
               :manual)
              :to-be
              token)
      (expect (resilience-kit:cancellation-token-reason token)
              :to-be
              :manual)
      (resilience-kit:cancel-cancellation-token token :later)
      (expect (resilience-kit:cancellation-token-reason token)
              :to-be
              :manual)))

  (it "merges an omitted context operation from its parent"
    (let* ((base (resilience-kit:make-resilience-context
                  :operation
                  :base))
           (overlay (resilience-kit:make-resilience-context
                     :correlation-id
                     "inner"))
           (merged (resilience-kit:merge-resilience-context
                    base
                    overlay)))
      (expect (resilience-kit:resilience-context-operation merged)
              :to-be
              :base)))

  (it "allows an explicitly unbounded deadline call"
    (expect (resilience-kit:call-with-deadline
             (lambda () :unbounded))
            :to-be
            :unbounded))

  (it "rejects nil lease owners and invalid breaker reset timeouts"
    (let ((store (resilience-kit:make-memory-lease-store)))
      (expect-error-forms
       (resilience-kit:acquire-resilience-lease
        store
        "nil-owner"
        nil)))
    (expect-invalid-calls (#'resilience-kit:make-circuit-breaker)
      (:reset-timeout 0)
      (:half-open-probe-limit 0)
      (:success-threshold 0)
      (:condition-classifier 1)
      (:result-classifier 1)))

  (it "uses the timed condition wait path while draining"
    (let ((lifecycle (resilience-kit:make-resilience-lifecycle)))
      (resilience-kit:enter-resilience-lifecycle lifecycle :operation :timed-wait)
      (resilience-kit:begin-resilience-drain lifecycle)
      (expect (resilience-kit:await-resilience-drained
               lifecycle
               :timeout
               0.01d0)
              :to-be
              nil)
      (expect (resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be
              0)
      (expect (resilience-kit:stop-resilience-lifecycle lifecycle)
              :to-be-truthy))))

  (it "coordinates lifecycle draining and health readiness"
    (let ((lifecycle (resilience-kit:make-resilience-lifecycle)))
      (resilience-kit:enter-resilience-lifecycle lifecycle
                                                    :operation :read)
      (expect (resilience-kit:stop-resilience-lifecycle lifecycle
                                                           :timeout 0)
              :to-be
              nil)
      (expect (resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :draining)
      (resilience-kit:leave-resilience-lifecycle lifecycle)
      (expect (resilience-kit:stop-resilience-lifecycle lifecycle)
              :to-be-truthy)
      (expect (resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :stopped)
      (let ((condition
              (expect-condition
               (lambda ()
                 (resilience-kit:enter-resilience-lifecycle lifecycle))
               'resilience-kit:resilience-draining)))
        (expect condition :to-be-truthy)))
    (let ((registry (resilience-kit:make-health-registry)))
      (resilience-kit:register-health-check
       registry :database (lambda () (error "database unavailable")))
      (let ((report (resilience-kit:health-report registry)))
        (expect report :to-be-truthy)
        (expect (getf (first report) :status) :to-be :unhealthy)
        (expect (getf (first report) :condition) :to-be-truthy))
      (expect (resilience-kit:health-ready-p registry) :to-be nil)
      (expect (resilience-kit:health-live-p registry) :to-be-truthy)
      (expect (resilience-kit:unregister-health-check registry :database)
              :to-be-truthy)
      (expect (resilience-kit:health-report registry) :to-be nil)))

  (it "rejects unsafe hedging and reports all failed attempts"
    (let ((condition
            (expect-condition
             (lambda ()
               (resilience-kit:call-with-hedging
                (lambda () :ok)
                :max-attempts 2))
             'resilience-kit:hedge-unsafe)))
      (expect condition :to-be-truthy))
    (let ((condition
            (expect-condition
             (lambda ()
               (resilience-kit:call-with-hedging
                (lambda () (error "hedged failure"))
                :hedge-safe-p t
                :max-attempts 2
                :hedge-after 0d0))
             'resilience-kit:hedge-exhausted)))
      (expect condition :to-be-truthy)
      (expect (resilience-kit:hedge-exhausted-attempts condition)
              :to-be
              2)
      (expect (length (resilience-kit:hedge-exhausted-causes condition))
              :to-be
              2)))

  (it "supports block macros for execution boundaries"
    (expect
     (resilience-kit:with-hedging
         (:max-attempts 1 :operation :macro-hedge)
       :hedged)
     :to-be
     :hedged)
    (let ((coalescer (resilience-kit:make-request-coalescer)))
      (expect
       (resilience-kit:with-request-coalescing
           (coalescer :key "macro-key" :operation :macro-coalesce)
         :coalesced)
       :to-be
       :coalesced)
      (expect (resilience-kit:request-coalescer-size coalescer)
              :to-be
              0))
    (with-test-executor (executor :size 1)
      (expect
       (resilience-kit:with-resilience-executor
           (executor :timeout 1d0 :operation :macro-executor)
         :executed)
       :to-be
       :executed))
    (expect-invalid-calls (#'resilience-kit:make-resilience-executor)
      (:queue-capacity 0)))

  (it "exposes executor health metrics through the public contract"
    (with-test-executor (executor :size 1)
      (expect (resilience-kit:resilience-executor-queue-depth executor)
              :to-be
              0)
      (expect (resilience-kit:resilience-executor-queue-capacity executor)
              :to-be
              nil)
      (expect (resilience-kit:resilience-executor-high-water-mark executor)
              :to-be
              0)
      (expect (resilience-kit:resilience-executor-shutdown-p executor)
              :to-be
              nil)
      (expect (resilience-kit:resilience-executor-terminated-p executor)
              :to-be
              nil)))

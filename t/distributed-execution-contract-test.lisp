(in-package #:resilience-kit/test)

(describe "leased and asynchronous boundary contracts"
  (it "executes leased distributed calls exactly once"
    (let* ((store (resilience-kit:make-memory-state-store))
           (lease-store (resilience-kit:make-memory-lease-store))
           (calls 0)
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "lease-once"
              :lease-store lease-store
              :lease-owner "node-a")))
      (expect (resilience-kit:distributed-circuit-breaker-call
               breaker
               (lambda () (incf calls)))
              :to-be
              1)
      (expect calls :to-be 1)))

  (it "reports distributed lease contention"
    (let* ((store (resilience-kit:make-memory-state-store))
           (lease-store (resilience-kit:make-memory-lease-store))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "lease-contention"
              :lease-store lease-store
              :lease-owner "node-a"))
           (external-lease
             (resilience-kit:acquire-resilience-lease
              lease-store "lease-contention" "node-b" :ttl 10)))
      (unwind-protect
           (expect-condition
            (lambda ()
              (resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :unreachable)))
            'resilience-kit:resilience-lease-unavailable)
        (resilience-kit:release-resilience-lease external-lease))))

  (it "keeps distributed cancellation and classifier failures observable"
    (let* ((store (resilience-kit:make-memory-state-store))
           (lease-store (resilience-kit:make-memory-lease-store))
           (token (resilience-kit:make-cancellation-token))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "cancelled"
              :lease-store lease-store
              :lease-owner "node-a")))
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker
          (lambda ()
            (resilience-kit:cancel-cancellation-token token)
            :cancelled)
          :cancellation-token token))
       'resilience-kit:resilience-cancelled)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:distributed-circuit-breaker-failure-count
               breaker)
              :to-be
              0))
    (let ((breaker
            (resilience-kit:make-distributed-circuit-breaker
             :store (resilience-kit:make-memory-state-store)
             :key "condition-classifier-error"
             :failure-threshold 1
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               (error "distributed condition classifier failure")))))
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "distributed operation failure"))))
       'error)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)
      (expect (resilience-kit:distributed-circuit-breaker-opened-at breaker)
              :to-be-truthy))
    (let ((breaker
            (resilience-kit:make-distributed-circuit-breaker
             :store (resilience-kit:make-memory-state-store)
             :key "below-threshold"
             :failure-threshold 2)))
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "one distributed failure"))))
       'error)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:distributed-circuit-breaker-failure-count
               breaker)
              :to-be
              1)))

  (it "exposes the distributed breaker snapshot accessors"
    (let ((breaker
            (resilience-kit:make-distributed-circuit-breaker
             :store (resilience-kit:make-memory-state-store)
             :key "snapshot")))
      (expect (resilience-kit:distributed-circuit-breaker-failure-count
               breaker)
              :to-be
              0)
      (expect (resilience-kit:distributed-circuit-breaker-opened-at breaker)
              :to-be
              nil)
      (expect (resilience-kit:distributed-circuit-breaker-active-probes
               breaker)
              :to-be
              0)
      (expect (resilience-kit:distributed-circuit-breaker-generation breaker)
              :to-be
              0)))

  (it "composes distributed and asynchronous execution boundaries"
    (let* ((metrics (resilience-kit:make-resilience-metrics))
           (observed 0)
           (observer
             (resilience-kit:make-resilience-observer
              (lambda (event)
                (declare (ignore event))
                (incf observed))))
           (lifecycle (resilience-kit:make-resilience-lifecycle))
           (store (resilience-kit:make-memory-state-store))
           (executor (resilience-kit:make-resilience-executor :size 1))
           (coalescer (resilience-kit:make-request-coalescer)))
      (unwind-protect
           (progn
             (expect
              (resilience-kit:call-with-resilience
               (lambda () :distributed)
               :distributed-circuit-breaker
               (resilience-kit:make-distributed-circuit-breaker
                :store store
                :key "composition-distributed")
               :operation :distributed)
              :to-be
              :distributed)
             (expect
              (resilience-kit:call-with-resilience
               (lambda () :coalesced)
               :request-coalescer coalescer
               :executor executor
               :idempotency-key "composition-coalesced"
               :operation :coalesced)
              :to-be
              :coalesced)
             (expect
              (resilience-kit:call-with-resilience
               (lambda () :hedged)
               :max-hedge-attempts 2
               :hedge-safe-p t
               :idempotency-key "composition-hedged"
               :operation :hedged)
              :to-be
              :hedged)
             (expect
              (resilience-kit:call-with-resilience
               (lambda () :executed)
               :executor executor
               :operation :executed)
              :to-be
              :executed)
             (expect
              (resilience-kit:call-with-resilience
               (lambda () :observed)
               :event-handler (lambda (event)
                                (declare (ignore event)))
               :metrics metrics
               :observer observer
               :lifecycle lifecycle
               :operation :observed)
              :to-be
              :observed)
             (expect-condition
              (lambda ()
                (resilience-kit:call-with-resilience
                 (lambda () (error "composition failure"))
                 :metrics metrics
                 :observer observer
                 :lifecycle lifecycle
                 :operation :failed))
              'error)
             (expect (resilience-kit:resilience-lifecycle-active lifecycle)
                     :to-be
                     0)
             (expect (resilience-kit:resilience-metrics-total-events metrics)
                     :to-be-truthy)
             (expect observed :to-be-truthy)
             (expect (resilience-kit:request-coalescer-size coalescer)
                     :to-be
                     0))
        (resilience-kit:resilience-executor-shutdown executor :wait t))))
)

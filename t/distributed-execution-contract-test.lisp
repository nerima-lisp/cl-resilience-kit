(in-package #:cl-resilience-kit/test)

(describe "leased and asynchronous boundary contracts"
  (it "executes leased distributed calls exactly once"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (lease-store (cl-resilience-kit:make-memory-lease-store))
           (calls 0)
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "lease-once"
              :lease-store lease-store
              :lease-owner "node-a")))
      (expect (cl-resilience-kit:distributed-circuit-breaker-call
               breaker
               (lambda () (incf calls)))
              :to-be
              1)
      (expect calls :to-be 1)))

  (it "reports distributed lease contention"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (lease-store (cl-resilience-kit:make-memory-lease-store))
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "lease-contention"
              :lease-store lease-store
              :lease-owner "node-a"))
           (external-lease
             (cl-resilience-kit:acquire-resilience-lease
              lease-store "lease-contention" "node-b" :ttl 10)))
      (unwind-protect
           (expect-condition
            (lambda ()
              (cl-resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :unreachable)))
            'cl-resilience-kit:resilience-lease-unavailable)
        (cl-resilience-kit:release-resilience-lease external-lease))))

  (it "keeps distributed cancellation and classifier failures observable"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (lease-store (cl-resilience-kit:make-memory-lease-store))
           (token (cl-resilience-kit:make-cancellation-token))
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "cancelled"
              :lease-store lease-store
              :lease-owner "node-a")))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker
          (lambda ()
            (cl-resilience-kit:cancel-cancellation-token token)
            :cancelled)
          :cancellation-token token))
       'cl-resilience-kit:resilience-cancelled)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (cl-resilience-kit:distributed-circuit-breaker-failure-count
               breaker)
              :to-be
              0))
    (let ((breaker
            (cl-resilience-kit:make-distributed-circuit-breaker
             :store (cl-resilience-kit:make-memory-state-store)
             :key "condition-classifier-error"
             :failure-threshold 1
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               (error "distributed condition classifier failure")))))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "distributed operation failure"))))
       'error)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)
      (expect (cl-resilience-kit:distributed-circuit-breaker-opened-at breaker)
              :to-be-truthy))
    (let ((breaker
            (cl-resilience-kit:make-distributed-circuit-breaker
             :store (cl-resilience-kit:make-memory-state-store)
             :key "below-threshold"
             :failure-threshold 2)))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "one distributed failure"))))
       'error)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (cl-resilience-kit:distributed-circuit-breaker-failure-count
               breaker)
              :to-be
              1)))

  (it "exposes the distributed breaker snapshot accessors"
    (let ((breaker
            (cl-resilience-kit:make-distributed-circuit-breaker
             :store (cl-resilience-kit:make-memory-state-store)
             :key "snapshot")))
      (expect (cl-resilience-kit:distributed-circuit-breaker-failure-count
               breaker)
              :to-be
              0)
      (expect (cl-resilience-kit:distributed-circuit-breaker-opened-at breaker)
              :to-be
              nil)
      (expect (cl-resilience-kit:distributed-circuit-breaker-active-probes
               breaker)
              :to-be
              0)
      (expect (cl-resilience-kit:distributed-circuit-breaker-generation breaker)
              :to-be
              0)))

  (it "composes distributed and asynchronous execution boundaries"
    (let* ((metrics (cl-resilience-kit:make-resilience-metrics))
           (observed 0)
           (observer
             (cl-resilience-kit:make-resilience-observer
              (lambda (event)
                (declare (ignore event))
                (incf observed))))
           (lifecycle (cl-resilience-kit:make-resilience-lifecycle))
           (store (cl-resilience-kit:make-memory-state-store))
           (executor (cl-resilience-kit:make-resilience-executor :size 1))
           (coalescer (cl-resilience-kit:make-request-coalescer)))
      (unwind-protect
           (progn
             (expect
              (cl-resilience-kit:call-with-resilience
               (lambda () :distributed)
               :distributed-circuit-breaker
               (cl-resilience-kit:make-distributed-circuit-breaker
                :store store
                :key "composition-distributed")
               :operation :distributed)
              :to-be
              :distributed)
             (expect
              (cl-resilience-kit:call-with-resilience
               (lambda () :coalesced)
               :request-coalescer coalescer
               :executor executor
               :idempotency-key "composition-coalesced"
               :operation :coalesced)
              :to-be
              :coalesced)
             (expect
              (cl-resilience-kit:call-with-resilience
               (lambda () :hedged)
               :max-hedge-attempts 2
               :hedge-safe-p t
               :idempotency-key "composition-hedged"
               :operation :hedged)
              :to-be
              :hedged)
             (expect
              (cl-resilience-kit:call-with-resilience
               (lambda () :executed)
               :executor executor
               :operation :executed)
              :to-be
              :executed)
             (expect
              (cl-resilience-kit:call-with-resilience
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
                (cl-resilience-kit:call-with-resilience
                 (lambda () (error "composition failure"))
                 :metrics metrics
                 :observer observer
                 :lifecycle lifecycle
                 :operation :failed))
              'error)
             (expect (cl-resilience-kit:resilience-lifecycle-active lifecycle)
                     :to-be
                     0)
             (expect (cl-resilience-kit:resilience-metrics-total-events metrics)
                     :to-be-truthy)
             (expect observed :to-be-truthy)
             (expect (cl-resilience-kit:request-coalescer-size coalescer)
                     :to-be
                     0))
        (cl-resilience-kit:resilience-executor-shutdown executor :wait t))))
)

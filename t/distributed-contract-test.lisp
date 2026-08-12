(in-package #:resilience-kit/test)

(describe "distributed and asynchronous boundary contracts"
  (it "validates distributed state and completes fenced transitions"
    (let ((store (resilience-kit:make-memory-state-store)))
      (expect
       (lambda ()
         (resilience-kit:make-distributed-circuit-breaker
          :store store :key ""))
       :to-throw
       'error)
      (expect
       (lambda ()
         (resilience-kit:make-distributed-circuit-breaker
          :store store
          :key "lease-required"
          :lease-store (resilience-kit:make-memory-lease-store)))
       :to-throw
       'error)
      (expect
       (lambda ()
         (resilience-kit:make-distributed-circuit-breaker
          :store store
          :key "invalid-reset-timeout"
          :reset-timeout 0))
       :to-throw
       'error)
      (resilience-kit:state-store-put-if-version
       store "invalid" (list :state :broken) nil)
      (let ((breaker
              (resilience-kit:make-distributed-circuit-breaker
               :store store :key "invalid")))
        (expect-condition
         (lambda ()
           (resilience-kit:distributed-circuit-breaker-state breaker))
         'resilience-kit:resilience-store-error)))
    (let* ((fixture (make-test-fixture :start 0))
           (store (resilience-kit:make-memory-state-store))
           (lease-store
             (resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "distributed"
              :failure-threshold 1
              :reset-timeout 1
              :success-threshold 2
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+
              :lease-store lease-store
              :lease-owner "node-a"
              :lease-ttl 2)))
      (expect (resilience-kit:distributed-circuit-breaker-key breaker)
              :to-be
              "distributed")
      (expect (resilience-kit:distributed-circuit-breaker-lease-owner breaker)
              :to-be
              "node-a")
      (expect (approximately-equal-p
               (resilience-kit:distributed-circuit-breaker-lease-ttl breaker)
               2)
              :to-be-truthy)
      (expect
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "distributed failure"))))
       :to-throw
       'error)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)
      (expect (resilience-kit:distributed-circuit-breaker-reset breaker)
              :to-be
              breaker)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:with-distributed-circuit-breaker
                   (breaker :operation :macro)
                 :macro-result)
              :to-be
              :macro-result)
      (let ((other-lease
              (resilience-kit:acquire-resilience-lease
               lease-store "distributed" "node-b" :ttl 1)))
        (expect other-lease :to-be-truthy)
        (expect (resilience-kit:release-resilience-lease other-lease)
                :to-be-truthy))
      (expect
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "open again"))))
       :to-throw
       'error)
      (advance-fixture fixture 1)
      (expect (resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :probe-one))
              :to-be
              :probe-one)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :half-open)
      (expect (resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :probe-two))
              :to-be
              :probe-two)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed))
    (let ((breaker
            (resilience-kit:make-distributed-circuit-breaker
             :store (resilience-kit:make-memory-state-store)
             :key "classified"
             :failure-threshold 1
             :result-classifier
             (lambda (value attempt)
               (declare (ignore attempt))
               (eq value :bad)))))
      (expect (resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :bad))
              :to-be
              :bad)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open))
    (let ((breaker
            (resilience-kit:make-distributed-circuit-breaker
             :store (resilience-kit:make-memory-state-store)
             :key "classifier-error"
             :failure-threshold 1
             :result-classifier
             (lambda (value attempt)
               (declare (ignore value attempt))
               (error "distributed classifier failure")))))
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () :ok)))
       'error)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)))

  (it "merges context and derives hedge idempotency"
    (let* ((overlay
             (resilience-kit:make-resilience-context
              :operation :overlay
              :idempotency-key "overlay"
              :tags '((:overlay . t))))
           (base
             (resilience-kit:make-resilience-context
              :operation :base
              :idempotency-key "base"
              :tags '((:base . t))
              :baggage '((:trace . "one"))))
           (merged (resilience-kit:merge-resilience-context base overlay)))
      (expect (resilience-kit:merge-resilience-context nil overlay)
              :to-be
              overlay)
      (expect (resilience-kit:resilience-context-operation merged)
              :to-be
              :overlay)
      (expect (resilience-kit:resilience-context-idempotency-key merged)
              :to-be
              "overlay")
      (expect (length (resilience-kit:resilience-context-tags merged))
              :to-be
              2)
      (expect (resilience-kit:resilience-context-baggage merged)
              :to-equal
              '((:trace . "one"))))
    (expect-condition
     (lambda ()
       (resilience-kit:call-with-hedging
        (lambda () :ok)
        :hedge-after -1d0))
     'error)
    (expect-condition
     (lambda ()
       (resilience-kit:call-with-hedging
        (lambda () :ok)
        :max-attempts 0))
     'type-error)
    (expect (resilience-kit:call-with-hedging
             (lambda () :single)
             :max-attempts 1)
            :to-be
            :single)
    (resilience-kit:with-resilience-context
        (:operation :parent
         :idempotency-key "request-1"
         :tags '((:parent . t)))
      (resilience-kit:with-resilience-context
          (:operation :child
           :tags '((:child . t)))
        (let ((context (resilience-kit:current-resilience-context)))
          (expect (resilience-kit:resilience-context-operation context)
                  :to-be
                  :child)
          (expect (resilience-kit:resilience-context-idempotency-key context)
                  :to-be
                  "request-1")
          (expect (length (resilience-kit:resilience-context-tags context))
                  :to-be
                  2))
        (expect (resilience-kit:call-with-hedging
                 (lambda () :hedged)
                 :hedge-after 0d0
                 :max-attempts 2)
                :to-be
                :hedged))))

  (it "keeps the portable state-store contract explicit"
    (let ((store (make-instance 'resilience-kit:resilience-state-store)))
      (dolist (operation
                (list
                 (lambda ()
                   (resilience-kit:state-store-put-if-version
                    store "missing" nil nil))
                 (lambda ()
                   (resilience-kit:state-store-delete-if-version
                    store "missing" nil))
                 (lambda ()
                   (resilience-kit:state-store-scan-prefix
                    store "missing/"))))
        (expect-condition operation
                           'resilience-kit:resilience-store-error))))

  (it "enforces rate-limiter capacity and waiting boundaries"
    (let* ((fixture (make-test-fixture :start 0))
           (limiter
             (resilience-kit:make-rate-limiter
              :capacity 2
              :initial-tokens 0
              :refill-rate 1
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+
              :sleeper (test-fixture-sleeper fixture))))
      (expect-condition
       (lambda ()
         (resilience-kit:rate-limiter-acquire limiter :tokens 0))
       'error)
      (expect-condition
       (lambda ()
         (resilience-kit:rate-limiter-acquire limiter :max-wait -1))
       'error)
      (multiple-value-bind (acquired retry-after)
          (resilience-kit:rate-limiter-acquire limiter :tokens 3)
        (expect acquired :to-be nil)
        (expect retry-after :to-be nil))
      (expect-condition
       (lambda ()
         (resilience-kit:rate-limiter-acquire
          limiter :tokens 3 :signal-on-reject-p t))
       'resilience-kit:rate-limit-exceeded)
      (multiple-value-bind (acquired retry-after)
          (resilience-kit:rate-limiter-acquire
           limiter :tokens 1 :wait-p t :max-wait 0.5d0)
        (expect acquired :to-be nil)
        (expect (approximately-equal-p retry-after 1)
                :to-be-truthy))
      (let ((non-refilling
              (resilience-kit:make-rate-limiter
               :capacity 1
               :initial-tokens 0
               :refill-rate 0
               :clock (test-fixture-clock fixture)
               :monotonic-units-per-second
               +test-monotonic-units-per-second+
               :sleeper (test-fixture-sleeper fixture))))
        (multiple-value-bind (acquired retry-after)
            (resilience-kit:rate-limiter-acquire
             non-refilling :wait-p t :max-wait 1)
          (expect acquired :to-be nil)
          (expect retry-after :to-be nil)))))
)

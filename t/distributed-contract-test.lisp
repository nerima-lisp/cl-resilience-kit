(in-package #:cl-resilience-kit/test)

(describe "distributed and asynchronous boundary contracts"
  (it "validates distributed state and completes fenced transitions"
    (let ((store (cl-resilience-kit:make-memory-state-store)))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:make-distributed-circuit-breaker
          :store store :key ""))
       'error)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:make-distributed-circuit-breaker
          :store store
          :key "lease-required"
          :lease-store (cl-resilience-kit:make-memory-lease-store)))
       'error)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:make-distributed-circuit-breaker
          :store store
          :key "invalid-reset-timeout"
          :reset-timeout 0))
       'error)
      (cl-resilience-kit:state-store-put-if-version
       store "invalid" (list :state :broken) nil)
      (let ((breaker
              (cl-resilience-kit:make-distributed-circuit-breaker
               :store store :key "invalid")))
        (expect-condition
         (lambda ()
           (cl-resilience-kit:distributed-circuit-breaker-state breaker))
         'cl-resilience-kit:resilience-store-error)))
    (let* ((fixture (make-test-fixture :start 0))
           (store (cl-resilience-kit:make-memory-state-store))
           (lease-store
             (cl-resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
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
      (expect (cl-resilience-kit:distributed-circuit-breaker-key breaker)
              :to-be
              "distributed")
      (expect (cl-resilience-kit:distributed-circuit-breaker-lease-owner breaker)
              :to-be
              "node-a")
      (expect (approximately-equal-p
               (cl-resilience-kit:distributed-circuit-breaker-lease-ttl breaker)
               2)
              :to-be-truthy)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "distributed failure"))))
       'error)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)
      (expect (cl-resilience-kit:distributed-circuit-breaker-reset breaker)
              :to-be
              breaker)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (cl-resilience-kit:with-distributed-circuit-breaker
                   (breaker :operation :macro)
                 :macro-result)
              :to-be
              :macro-result)
      (let ((other-lease
              (cl-resilience-kit:acquire-resilience-lease
               lease-store "distributed" "node-b" :ttl 1)))
        (expect other-lease :to-be-truthy)
        (expect (cl-resilience-kit:release-resilience-lease other-lease)
                :to-be-truthy))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "open again"))))
       'error)
      (advance-fixture fixture 1)
      (expect (cl-resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :probe-one))
              :to-be
              :probe-one)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :half-open)
      (expect (cl-resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :probe-two))
              :to-be
              :probe-two)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed))
    (let ((breaker
            (cl-resilience-kit:make-distributed-circuit-breaker
             :store (cl-resilience-kit:make-memory-state-store)
             :key "classified"
             :failure-threshold 1
             :result-classifier
             (lambda (value attempt)
               (declare (ignore attempt))
               (eq value :bad)))))
      (expect (cl-resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :bad))
              :to-be
              :bad)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open))
    (let ((breaker
            (cl-resilience-kit:make-distributed-circuit-breaker
             :store (cl-resilience-kit:make-memory-state-store)
             :key "classifier-error"
             :failure-threshold 1
             :result-classifier
             (lambda (value attempt)
               (declare (ignore value attempt))
               (error "distributed classifier failure")))))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () :ok)))
       'error)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)))

  (it "merges context and derives hedge idempotency"
    (let* ((overlay
             (cl-resilience-kit:make-resilience-context
              :operation :overlay
              :idempotency-key "overlay"
              :tags '((:overlay . t))))
           (base
             (cl-resilience-kit:make-resilience-context
              :operation :base
              :idempotency-key "base"
              :tags '((:base . t))
              :baggage '((:trace . "one"))))
           (merged (cl-resilience-kit:merge-resilience-context base overlay)))
      (expect (cl-resilience-kit:merge-resilience-context nil overlay)
              :to-be
              overlay)
      (expect (cl-resilience-kit:resilience-context-operation merged)
              :to-be
              :overlay)
      (expect (cl-resilience-kit:resilience-context-idempotency-key merged)
              :to-be
              "overlay")
      (expect (length (cl-resilience-kit:resilience-context-tags merged))
              :to-be
              2)
      (expect (cl-resilience-kit:resilience-context-baggage merged)
              :to-equal
              '((:trace . "one"))))
    (expect-condition
     (lambda ()
       (cl-resilience-kit:call-with-hedging
        (lambda () :ok)
        :hedge-after -1d0))
     'error)
    (expect-condition
     (lambda ()
       (cl-resilience-kit:call-with-hedging
        (lambda () :ok)
        :max-attempts 0))
     'type-error)
    (expect (cl-resilience-kit:call-with-hedging
             (lambda () :single)
             :max-attempts 1)
            :to-be
            :single)
    (cl-resilience-kit:with-resilience-context
        (:operation :parent
         :idempotency-key "request-1"
         :tags '((:parent . t)))
      (cl-resilience-kit:with-resilience-context
          (:operation :child
           :tags '((:child . t)))
        (let ((context (cl-resilience-kit:current-resilience-context)))
          (expect (cl-resilience-kit:resilience-context-operation context)
                  :to-be
                  :child)
          (expect (cl-resilience-kit:resilience-context-idempotency-key context)
                  :to-be
                  "request-1")
          (expect (length (cl-resilience-kit:resilience-context-tags context))
                  :to-be
                  2))
        (expect (cl-resilience-kit:call-with-hedging
                 (lambda () :hedged)
                 :hedge-after 0d0
                 :max-attempts 2)
                :to-be
                :hedged))))

  (it "keeps the portable state-store contract explicit"
    (let ((store (make-instance 'cl-resilience-kit:resilience-state-store)))
      (dolist (operation
                (list
                 (lambda ()
                   (cl-resilience-kit:state-store-put-if-version
                    store "missing" nil nil))
                 (lambda ()
                   (cl-resilience-kit:state-store-delete-if-version
                    store "missing" nil))
                 (lambda ()
                   (cl-resilience-kit:state-store-scan-prefix
                    store "missing/"))))
        (expect-condition operation
                           'cl-resilience-kit:resilience-store-error))))

  (it "enforces rate-limiter capacity and waiting boundaries"
    (let* ((fixture (make-test-fixture :start 0))
           (limiter
             (cl-resilience-kit:make-rate-limiter
              :capacity 2
              :initial-tokens 0
              :refill-rate 1
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+
              :sleeper (test-fixture-sleeper fixture))))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:rate-limiter-acquire limiter :tokens 0))
       'error)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:rate-limiter-acquire limiter :max-wait -1))
       'error)
      (multiple-value-bind (acquired retry-after)
          (cl-resilience-kit:rate-limiter-acquire limiter :tokens 3)
        (expect acquired :to-be nil)
        (expect retry-after :to-be nil))
      (expect-condition
       (lambda ()
         (cl-resilience-kit:rate-limiter-acquire
          limiter :tokens 3 :signal-on-reject-p t))
       'cl-resilience-kit:rate-limit-exceeded)
      (multiple-value-bind (acquired retry-after)
          (cl-resilience-kit:rate-limiter-acquire
           limiter :tokens 1 :wait-p t :max-wait 0.5d0)
        (expect acquired :to-be nil)
        (expect (approximately-equal-p retry-after 1)
                :to-be-truthy))
      (let ((non-refilling
              (cl-resilience-kit:make-rate-limiter
               :capacity 1
               :initial-tokens 0
               :refill-rate 0
               :clock (test-fixture-clock fixture)
               :monotonic-units-per-second
               +test-monotonic-units-per-second+
               :sleeper (test-fixture-sleeper fixture))))
        (multiple-value-bind (acquired retry-after)
            (cl-resilience-kit:rate-limiter-acquire
             non-refilling :wait-p t :max-wait 1)
          (expect acquired :to-be nil)
          (expect retry-after :to-be nil)))))

  (it "rejects malformed persisted distributed breaker state"
    (dolist (entry (list
                    (list "not-a-list" :broken)
                    (list "negative-count"
                          (list :state :closed :failure-count -1))
                    (list "bad-opened-at"
                          (list :state :open :opened-at :not-a-time))))
      (let ((store (cl-resilience-kit:make-memory-state-store))
            (key (first entry)))
        (cl-resilience-kit:state-store-put-if-version
         store key (second entry) nil)
        (let ((breaker
                (cl-resilience-kit:make-distributed-circuit-breaker
                 :store store :key key)))
          (expect-condition
           (lambda ()
             (cl-resilience-kit:distributed-circuit-breaker-state breaker))
           'cl-resilience-kit:resilience-store-error)))))

  (it "surfaces distributed state-store initialization failure"
    (let ((store
            (make-instance
             'controlled-conflict-state-store
             :delegate (cl-resilience-kit:make-memory-state-store)
             :remaining-conflicts 65)))
      (let ((breaker
              (cl-resilience-kit:make-distributed-circuit-breaker
               :store store
               :key "initialization-conflicts")))
        (expect-condition
         (lambda ()
           (cl-resilience-kit:distributed-circuit-breaker-call
            breaker (lambda () :unreachable)))
         'cl-resilience-kit:resilience-store-error))))

  (it "rejects calls while all distributed half-open probes are active"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (key "half-open-probe-limit")
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :half-open-probe-limit 1)))
      (seed-distributed-state
       store key
       :state :half-open
       :active-probes 1
       :generation 7)
      (let ((rejection
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:distributed-circuit-breaker-call
                  breaker (lambda () :unreachable) :operation :probe))
               'cl-resilience-kit:circuit-open)))
        (expect (cl-resilience-kit:circuit-open-state rejection)
                :to-be
                :half-open))))

  (it "retries fenced distributed transitions after CAS conflicts"
    (let* ((delegate (cl-resilience-kit:make-memory-state-store))
           (key "open-transition-conflict")
           (fixture (make-test-fixture :start 1))
           (store
             (make-instance
              'controlled-conflict-state-store
              :delegate delegate
              :remaining-conflicts 1))
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :reset-timeout 1
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (seed-distributed-state
       delegate key :state :open :opened-at 0 :generation 3)
      (expect (cl-resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :recovered))
              :to-be
              :recovered)
      (expect (controlled-conflict-state-store-remaining-conflicts store)
              :to-be
              0)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (cl-resilience-kit:distributed-circuit-breaker-generation breaker)
              :to-be
              5)))

  (it "ignores stale distributed completion tokens"
    (let ((store (cl-resilience-kit:make-memory-state-store)))
      (dolist (entry '((:generation . 99) (:state . :open)))
        (let* ((field (car entry))
               (value (cdr entry))
               (key (format nil "stale-token-~A" field))
               (breaker
                 (cl-resilience-kit:make-distributed-circuit-breaker
                  :store store
                  :key key)))
          (seed-distributed-state store key)
          (expect
           (cl-resilience-kit:distributed-circuit-breaker-call
            breaker
            (lambda ()
              (multiple-value-bind (state version)
                  (cl-resilience-kit:state-store-get store key)
                (setf (getf state field) value)
                (when (eq field :state)
                  (setf (getf state :opened-at) 0))
                (cl-resilience-kit:state-store-put-if-version
                 store key state version)
                :stale)))
           :to-be
           :stale)
          (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
                  :to-be
                  (if (eq field :state) :open :closed))))))

  (it "records a failure after completion CAS exhaustion"
    (let* ((delegate (cl-resilience-kit:make-memory-state-store))
           (key "completion-conflicts")
           (store
             (make-instance
              'controlled-conflict-state-store
              :delegate delegate
              :remaining-conflicts 64))
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :failure-threshold 1)))
      (seed-distributed-state delegate key)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () :completion-failure)))
       'cl-resilience-kit:resilience-store-error)
      (expect (controlled-conflict-state-store-remaining-conflicts store)
              :to-be
              0)
      (expect (cl-resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open)))
)

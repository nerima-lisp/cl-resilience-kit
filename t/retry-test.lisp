(in-package #:cl-resilience-kit/test)

(describe "retry policies"
  (it "retries classified conditions up to max-attempts"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 3
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (< attempts 3)
              (error "transient")
              :ok))
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :ok)
      (expect attempts :to-be 3)))

  (it "does not retry an unclassified condition"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (caught nil)
           (policy (make-retry-policy
                    :max-attempts 5
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      nil))))
      (handler-case
          (call-with-retry
           policy
           (lambda ()
             (incf attempts)
             (error "permanent"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (error (condition)
          (setf caught condition)))
      (expect attempts :to-be 1)
      (expect (typep caught 'error) :to-be-truthy)))

  (it "classifies returned results without an HTTP-specific type"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :result-classifier
                    (lambda (result attempt)
                      (declare (ignore attempt))
                      (eq result :retry)))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (= attempts 1) :retry :ok))
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :ok)
      (expect attempts :to-be 2)))

  (it "keeps an unsafe operation single-attempt by default"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (classifier-calls 0)
           (caught nil)
           (policy (make-retry-policy
                    :max-attempts 5
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (incf classifier-calls)
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda ()
             (incf attempts)
             (error "write failed"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (error (condition)
          (setf caught condition)))
      (expect attempts :to-be 1)
      (expect classifier-calls :to-be 0)
      (expect (typep caught 'error) :to-be-truthy)))

  (it "signals structured retry exhaustion"
    (let* ((fixture (make-test-fixture))
           (exhausted nil)
           (policy (make-retry-policy
                    :max-attempts 3
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda () (error "always fails"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (retry-exhausted (condition)
          (setf exhausted condition)))
      (expect (typep exhausted 'retry-exhausted) :to-be-truthy)
      (expect (retry-exhausted-attempts exhausted) :to-be 3)
      (expect (typep (retry-exhausted-last-condition exhausted) 'error)
              :to-be-truthy)))

  (it "retains a classifier reason in retry exhaustion"
    (let* ((fixture (make-test-fixture))
           (exhausted nil)
           (policy (make-retry-policy
                    :max-attempts 1
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (make-retry-decision
                       :retry-p t
                       :reason :classified-transient)))))
      (handler-case
          (call-with-retry
           policy
           (lambda () (error "classified failure"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (retry-exhausted (condition)
          (setf exhausted condition)))
      (expect (typep exhausted 'retry-exhausted) :to-be-truthy)
      (expect (retry-exhausted-reason exhausted)
              :to-be
              :classified-transient)))

  (it "records injected sleeps and applies the backoff cap"
    (let* ((fixture (make-test-fixture))
           (exhausted nil)
           (policy (make-retry-policy
                    :max-attempts 3
                    :initial-delay 1
                    :multiplier 2
                    :max-delay 1.5
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda () (error "always fails"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (retry-exhausted (condition)
          (setf exhausted condition)))
      (expect (typep exhausted 'retry-exhausted) :to-be-truthy)
      (expect (fixture-sleeps fixture) :to-equal '(1.0d0 1.5d0))
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 10
         :multiplier 3
         :max-delay 15)
        2)
       :to-be
       15.0d0)))

  (it "makes every jitter strategy deterministic with an injected source"
    (let ((fixture (make-test-fixture :random-values '(500000 500000 500000))))
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 10
         :jitter :full
         :random-source (test-fixture-random-source fixture))
        1)
       :to-be
       5.0d0)
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 10
         :jitter :equal
         :random-source (test-fixture-random-source fixture))
        1)
       :to-be
       7.5d0)
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 2
         :jitter :decorrelated
         :random-source (test-fixture-random-source fixture))
        2
        :previous-delay 2)
       :to-be
       4.0d0)))

  (it "returns a normalized retry decision without sleeping"
    (let ((policy (make-retry-policy
                   :max-attempts 2
                   :retry-safe-p t
                   :condition-classifier
                   (lambda (condition attempt)
                     (declare (ignore condition attempt))
                     (values t 0.25)))))
      (multiple-value-bind (retry-p decision)
          (retry-policy-should-retry-p
           policy
           1
           :condition (make-condition 'simple-error
                                      :format-control "transient"))
        (expect retry-p :to-be-truthy)
        (expect (typep decision 'cl-resilience-kit:retry-decision)
                :to-be-truthy))))

  (it "provides a block-oriented retry macro"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (with-retry (policy
                    :clock (test-fixture-clock fixture)
                    :monotonic-units-per-second
                    +test-monotonic-units-per-second+
                    :sleeper (test-fixture-sleeper fixture))
         (incf attempts)
         (if (= attempts 1)
             (error "transient")
             :ok))
       :to-be
       :ok)
      (expect attempts :to-be 2)))

  (it-property "backoff never exceeds its configured maximum"
      ((retry-number (gen-integer :min 1 :max 8))
       (maximum (gen-integer :min 0 :max 20)))
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier 2
                   :max-delay maximum)))
      (expect (<= (compute-backoff-delay policy retry-number)
                  (float maximum 1d0))
              :to-be-truthy)))

  (it "supports continuation-oriented test bodies"
    (with-continuation-result (result next calledp)
        ((lambda (continuation)
           (funcall continuation :ok :ignored))
         #'next)
      (expect calledp :to-be-truthy)
      (expect result :to-be :ok)))

  (it "limits retry authorization with a fixed-window budget"
    (let* ((fixture (make-test-fixture))
           (budget (make-retry-budget
                    :limit 2
                    :window 10
                    :clock (test-fixture-clock fixture)
                    :monotonic-units-per-second
                    +test-monotonic-units-per-second+)))
      (expect (retry-budget-used budget) :to-be 0)
      (expect (retry-budget-remaining budget) :to-be 2)
      (expect (retry-budget-acquire budget) :to-be-truthy)
      (expect (retry-budget-acquire budget) :to-be-truthy)
      (expect (retry-budget-acquire budget) :to-be nil)
      (expect (retry-budget-used budget) :to-be 2)
      (advance-fixture fixture 10)
      (expect (retry-budget-remaining budget) :to-be 2)
      (expect (retry-budget-acquire budget) :to-be-truthy)
      (expect (retry-budget-used budget) :to-be 1)))

  (it "stops retries when the shared budget is exhausted"
    (let* ((fixture (make-test-fixture))
           (budget (make-retry-budget
                    :limit 1
                    :window 10
                    :clock (test-fixture-clock fixture)
                    :monotonic-units-per-second
                    +test-monotonic-units-per-second+))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 5
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t)))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda ()
                         (incf attempts)
                         (error "transient"))
                       :retry-budget budget
                       :clock (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper (test-fixture-sleeper fixture)))
                    'retry-exhausted)))
      (expect attempts :to-be 2)
      (expect (retry-budget-used budget) :to-be 1)
      (expect (retry-exhausted-reason caught)
              :to-be
              :retry-budget-exhausted)
      (expect (retry-exhausted-attempts caught) :to-be 2)))

  (it "publishes attempt events and tolerates observer failures"
    (let* ((fixture (make-test-fixture))
           (events nil)
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (= attempts 1)
              (error "transient")
              :ok))
        :operation :read
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second
        +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture)
        :event-handler
        (lambda (event)
          (push event events)
          (when (eq (resilience-event-type event) :attempt-failure)
            (error "observer failure"))))
       :to-be
       :ok)
      (let ((types (mapcar #'resilience-event-type (reverse events))))
        (expect (count :attempt-start types) :to-be 2)
        (expect (count :attempt-failure types) :to-be 1)
        (expect (count :attempt-success types) :to-be 1)
        (expect (count :retry-scheduled types) :to-be 1)
        (expect (every (lambda (event)
                         (and (resilience-event-p event)
                              (eq (resilience-event-operation event) :read)))
                       events)
                :to-be-truthy))))

  (it "invokes a fallback when retry exhaustion is terminal"
    (let* ((fixture (make-test-fixture))
           (fallback-condition nil)
           (policy (make-retry-policy
                    :max-attempts 1
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-retry
        policy
        (lambda () (error "permanent"))
        :fallback (lambda (condition)
                    (setf fallback-condition condition)
                    :fallback)
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second
        +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :fallback)
      (expect (typep fallback-condition 'retry-exhausted)
              :to-be-truthy)
      (expect (retry-exhausted-attempts fallback-condition) :to-be 1)))

  (it "surfaces classifier failures as public resilience conditions"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 3
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (error "classifier failure"))))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda ()
                         (incf attempts)
                         (error "operation failure"))
                       :clock (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper (test-fixture-sleeper fixture)))
                    'retry-classifier-error)))
      (expect attempts :to-be 1)
      (expect (typep (retry-classifier-error-cause caught) 'error)
              :to-be-truthy)))

  (it "keeps exponential backoff finite under numeric overflow pressure"
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier most-positive-double-float)))
      (let ((delay (compute-backoff-delay policy 2)))
        (expect (floatp delay) :to-be-truthy)
        (expect (<= 0d0 delay most-positive-double-float)
                :to-be-truthy)))))

  (it "caps very large retry numbers without a linear delay loop"
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier 2
                   :max-delay 10)))
           (expect (compute-backoff-delay policy most-positive-fixnum)
              :to-be
              10d0)))

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

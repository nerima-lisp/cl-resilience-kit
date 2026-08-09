(in-package #:cl-resilience-kit/test)

(defclass controlled-conflict-state-store
    (cl-resilience-kit:resilience-state-store)
  ((delegate
    :initarg :delegate
    :reader controlled-conflict-state-store-delegate)
   (remaining-conflicts
    :initarg :remaining-conflicts
    :accessor controlled-conflict-state-store-remaining-conflicts)))

(defmethod cl-resilience-kit:state-store-get
    ((store controlled-conflict-state-store) key)
  (cl-resilience-kit:state-store-get
   (controlled-conflict-state-store-delegate store)
   key))

(defmethod cl-resilience-kit:state-store-put-if-version
    ((store controlled-conflict-state-store) key value expected-version)
  (if (plusp (controlled-conflict-state-store-remaining-conflicts store))
      (progn
        (decf (controlled-conflict-state-store-remaining-conflicts store))
        (error 'cl-resilience-kit:resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version expected-version))
      (cl-resilience-kit:state-store-put-if-version
       (controlled-conflict-state-store-delegate store)
       key
       value
       expected-version)))

(defun seed-distributed-state
    (store key &key (state :closed) (failure-count 0) opened-at
          (active-probes 0) (half-open-successes 0) (generation 0))
  (cl-resilience-kit:state-store-put-if-version
   store
   key
   (list :state state
         :failure-count failure-count
         :opened-at opened-at
         :active-probes active-probes
         :half-open-successes half-open-successes
         :generation generation)
   nil))

(describe "public contracts"
  (it "records metrics and isolates observer failures"
    (let* ((metrics (cl-resilience-kit:make-resilience-metrics))
           (event (cl-resilience-kit:make-resilience-event
                    :type :success :operation :read :duration 1.5d0))
           (undated (cl-resilience-kit:make-resilience-event
                      :type :success :operation :read))
           (calls 0)
           (observer
             (cl-resilience-kit:make-resilience-observer
              (lambda (ignored)
                (declare (ignore ignored))
                (error "observer failure"))
              (lambda (ignored)
                (declare (ignore ignored))
                (incf calls)))))
      (expect (cl-resilience-kit:record-resilience-event metrics event)
              :to-be-truthy)
      (cl-resilience-kit:record-resilience-event metrics undated)
      (expect (cl-resilience-kit:resilience-metrics-total-events metrics)
              :to-be 2)
      (expect (cl-resilience-kit:resilience-metrics-count
               metrics :success :operation :read)
              :to-be 2)
      (expect (cl-resilience-kit:resilience-metrics-duration
               metrics :success :operation :read)
              :to-be 1.5d0)
      (let ((snapshot (cl-resilience-kit:resilience-metrics-snapshot metrics)))
        (expect (getf snapshot :total-events) :to-be 2)
        (expect (length (getf snapshot :events)) :to-be 1)
        (expect (length (getf snapshot :durations)) :to-be 1))
      (expect (cl-resilience-kit:resilience-observer-handlers observer)
              :to-be-truthy)
      (let ((returned
              (funcall (cl-resilience-kit:resilience-observer-handler observer)
                       event)))
        (expect returned :to-be-truthy))
      (expect calls :to-be 1)
      (expect (cl-resilience-kit:reset-resilience-metrics metrics)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-metrics-total-events metrics)
              :to-be 0)))

  (it "uses injected clocks for breaker event timestamps"
    (let* ((fixture (make-test-fixture :start 42))
           (events nil)
           (breaker
             (cl-resilience-kit:make-circuit-breaker
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (cl-resilience-kit:circuit-breaker-call
        breaker (lambda () :ok)
        :operation :read
        :event-handler (lambda (event) (push event events)))
       :to-be
       :ok)
      (expect (length events) :to-be 1)
      (expect
       (approximately-equal-p
        (cl-resilience-kit:resilience-event-timestamp (first events))
        42)
       :to-be-truthy))
    (let* ((fixture (make-test-fixture :start 42))
           (store (cl-resilience-kit:make-memory-state-store))
           (events nil)
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "service-a"
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (cl-resilience-kit:distributed-circuit-breaker-call
        breaker (lambda () :ok)
        :operation :read
        :event-handler (lambda (event) (push event events)))
       :to-be
       :ok)
      (expect (length events) :to-be 1)
      (expect
       (approximately-equal-p
        (cl-resilience-kit:resilience-event-timestamp (first events))
        42)
       :to-be-truthy)))

  (it "preserves structured condition data"
    (let ((condition
            (make-condition 'cl-resilience-kit:deadline-exceeded
                            :operation :read
                            :deadline 5d0
                            :observed-at 5d0
                            :stage :wait
                            :attempt 2)))
      (expect (typep condition 'cl-resilience-kit:resilience-error)
              :to-be-truthy)
      (expect (cl-resilience-kit:deadline-exceeded-stage condition)
              :to-be
              :wait)
      (expect (cl-resilience-kit:deadline-exceeded-attempt condition)
              :to-be
              2)
      (expect (stringp (princ-to-string condition)) :to-be-truthy)))

  (it "copies state values and exposes the store protocol error"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (value (list :nested (list 1 2)))
           (original (list :nested (list 1 2)))
           (version
             (cl-resilience-kit:state-store-put-if-version
              store "state/a" value nil)))
      (expect version :to-be 1)
      (setf (second (getf value :nested)) 9)
      (multiple-value-bind (loaded loaded-version)
          (cl-resilience-kit:state-store-get store "state/a")
        (expect loaded-version :to-be 1)
        (expect loaded :to-equal original)
        (setf (second (getf loaded :nested)) 8))
      (multiple-value-bind (fresh fresh-version)
          (cl-resilience-kit:state-store-get store "state/a")
        (expect fresh-version :to-be 1)
        (expect fresh :to-equal original))
      (expect (length
               (cl-resilience-kit:state-store-scan-prefix store "state/"))
              :to-be 1)
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:state-store-get
                  (make-instance 'cl-resilience-kit:resilience-state-store)
                  "missing"))
               'cl-resilience-kit:resilience-store-error)))
        (expect condition :to-be-truthy)
        (expect (cl-resilience-kit:resilience-store-error-key condition)
                :to-be
                "missing"))))

  (it "enforces fencing lease ownership and supports soft contention"
    (let* ((fixture (make-test-fixture :start 10))
           (store
             (cl-resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (first-lease
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 10))
           (same-owner
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 10))
           (soft-contention
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-b" :ttl 10
              :signal-on-unavailable-p nil)))
      (expect first-lease :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lease-fencing-token same-owner)
              :to-be
              (cl-resilience-kit:resilience-lease-fencing-token first-lease))
      (expect soft-contention :to-be nil)
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:acquire-resilience-lease
                  store "lease/a" "owner-b" :ttl 10))
               'cl-resilience-kit:resilience-lease-unavailable)))
        (expect condition :to-be-truthy)
        (expect (cl-resilience-kit:resilience-lease-unavailable-retry-after
                 condition)
                :to-be-truthy))
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be-truthy)
      (expect (cl-resilience-kit:release-resilience-lease first-lease)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be
              nil)))

(describe "additional public boundary contracts"
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
                :hedged)))))

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

  (it "exposes lifecycle admission, timeout, and macro cleanup"
    (let ((lifecycle (cl-resilience-kit:make-resilience-lifecycle)))
      (expect (cl-resilience-kit:resilience-lifecycle-accepting-p lifecycle)
              :to-be-truthy)
      (cl-resilience-kit:enter-resilience-lifecycle lifecycle :operation :wait)
      (expect (cl-resilience-kit:await-resilience-drained
               lifecycle :timeout 0d0)
              :to-be
              nil)
      (expect (cl-resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be
              0)
      (expect
       (cl-resilience-kit:with-resilience-lifecycle
           (lifecycle :operation :macro)
         :macro-result)
       :to-be
       :macro-result)
      (cl-resilience-kit:begin-resilience-drain lifecycle)
      (expect (cl-resilience-kit:resilience-lifecycle-accepting-p lifecycle)
              :to-be
              nil)))

  (it "keeps structured condition payloads inspectable"
    (let* ((token (gensym "TOKEN"))
           (cause (gensym "CAUSE"))
           (result (gensym "RESULT"))
           (policy (gensym "POLICY"))
           (key (gensym "KEY"))
           (owner (gensym "OWNER"))
           (value (gensym "VALUE"))
           (specifications
             (list
              (list 'cl-resilience-kit:resilience-cancelled
                    (list :operation :cancel
                          :token token
                          :reason :shutdown)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :cancel)
                          (list #'cl-resilience-kit:resilience-cancelled-token
                                token)
                          (list #'cl-resilience-kit:resilience-cancelled-reason
                                :shutdown)))
              (list 'cl-resilience-kit:retry-exhausted
                    (list :operation :retry
                          :attempts 3
                          :last-condition cause
                          :last-result result
                          :reason :budget
                          :policy policy)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :retry)
                          (list #'cl-resilience-kit:retry-exhausted-attempts
                                3)
                          (list #'cl-resilience-kit:retry-exhausted-last-condition
                                cause)
                          (list #'cl-resilience-kit:retry-exhausted-last-result
                                result)
                          (list #'cl-resilience-kit:retry-exhausted-reason
                                :budget)
                          (list #'cl-resilience-kit:retry-exhausted-policy
                                policy)))
              (list 'cl-resilience-kit:retry-classifier-error
                    (list :operation :classify :cause cause)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :classify)
                          (list #'cl-resilience-kit:retry-classifier-error-cause
                                cause)))
              (list 'cl-resilience-kit:deadline-exceeded
                    (list :operation :deadline
                          :deadline 10
                          :observed-at 11
                          :stage :backoff
                          :attempt 2)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :deadline)
                          (list #'cl-resilience-kit:deadline-exceeded-deadline
                                10)
                          (list #'cl-resilience-kit:deadline-exceeded-observed-at
                                11)
                          (list #'cl-resilience-kit:deadline-exceeded-stage
                                :backoff)
                          (list #'cl-resilience-kit:deadline-exceeded-attempt
                                2)))
              (list 'cl-resilience-kit:attempt-timeout
                    (list :operation :attempt
                          :deadline 10
                          :observed-at 11
                          :stage :attempt
                          :attempt 1
                          :timeout 0.5d0)
                    (list (list #'cl-resilience-kit:attempt-timeout-timeout
                                0.5d0)))
              (list 'cl-resilience-kit:circuit-open
                    (list :operation :breaker
                          :state :half-open
                          :retry-at 12
                          :generation 3)
                    (list (list #'cl-resilience-kit:circuit-open-state
                                :half-open)
                          (list #'cl-resilience-kit:circuit-open-retry-at
                                12)
                          (list #'cl-resilience-kit:circuit-open-generation
                                3)))
              (list 'cl-resilience-kit:bulkhead-rejected
                    (list :operation :bulkhead :limit 4 :in-flight 4)
                    (list (list #'cl-resilience-kit:bulkhead-rejected-limit
                                4)
                          (list #'cl-resilience-kit:bulkhead-rejected-in-flight
                                4)))
              (list 'cl-resilience-kit:rate-limit-exceeded
                    (list :operation :rate-limit
                          :requested-tokens 3
                          :available-tokens 1
                          :retry-after 2.0d0)
                    (list (list #'cl-resilience-kit:rate-limit-exceeded-requested-tokens
                                3)
                          (list #'cl-resilience-kit:rate-limit-exceeded-available-tokens
                                1)
                          (list #'cl-resilience-kit:rate-limit-exceeded-retry-after
                                2.0d0)))
              (list 'cl-resilience-kit:resilience-store-error
                    (list :operation :store :key key :cause cause)
                    (list (list #'cl-resilience-kit:resilience-store-error-key
                                key)
                          (list #'cl-resilience-kit:resilience-store-error-cause
                                cause)))
              (list 'cl-resilience-kit:resilience-store-conflict
                    (list :operation :store
                          :key key
                          :cause cause
                          :expected-version 1
                          :actual-version 2)
                    (list (list #'cl-resilience-kit:resilience-store-conflict-expected-version
                                1)
                          (list #'cl-resilience-kit:resilience-store-conflict-actual-version
                                2)))
              (list 'cl-resilience-kit:resilience-lease-unavailable
                    (list :operation :lease
                          :key key
                          :owner owner
                          :retry-after 3.0d0)
                    (list (list #'cl-resilience-kit:resilience-lease-unavailable-key
                                key)
                          (list #'cl-resilience-kit:resilience-lease-unavailable-owner
                                owner)
                          (list #'cl-resilience-kit:resilience-lease-unavailable-retry-after
                                3.0d0)))
              (list 'cl-resilience-kit:resilience-lease-lost
                    (list :operation :lease
                          :key key
                          :owner owner
                          :fencing-token 4)
                    (list (list #'cl-resilience-kit:resilience-lease-lost-key
                                key)
                          (list #'cl-resilience-kit:resilience-lease-lost-owner
                                owner)
                          (list #'cl-resilience-kit:resilience-lease-lost-fencing-token
                                4)))
              (list 'cl-resilience-kit:stale-fencing-token
                    (list :operation :lease
                          :key key
                          :fencing-token 4
                          :current-fencing-token 5)
                    (list (list #'cl-resilience-kit:stale-fencing-token-key
                                key)
                          (list #'cl-resilience-kit:stale-fencing-token-fencing-token
                                4)
                          (list #'cl-resilience-kit:stale-fencing-token-current-fencing-token
                                5)))
              (list 'cl-resilience-kit:resilience-draining
                    (list :operation :lifecycle :state :draining)
                    (list (list #'cl-resilience-kit:resilience-draining-state
                                :draining)))
              (list 'cl-resilience-kit:resilience-execution-rejected
                    (list :operation :executor
                          :reason :queue-full
                          :queue-size 3)
                    (list (list #'cl-resilience-kit:resilience-execution-rejected-reason
                                :queue-full)
                          (list #'cl-resilience-kit:resilience-execution-rejected-queue-size
                                3)))
              (list 'cl-resilience-kit:resilience-execution-timeout
                    (list :operation :executor :timeout 2.0d0 :backend :thread)
                    (list (list #'cl-resilience-kit:resilience-execution-timeout-timeout
                                2.0d0)
                          (list #'cl-resilience-kit:resilience-execution-timeout-backend
                                :thread)))
              (list 'cl-resilience-kit:resilience-hard-timeout
                    (list :operation :executor
                          :deadline 10
                          :observed-at 11
                          :stage :attempt
                          :attempt 1
                          :timeout 2.0d0
                          :backend :executor)
                    (list (list #'cl-resilience-kit:resilience-hard-timeout-backend
                                :executor)))
              (list 'cl-resilience-kit:hedge-unsafe
                    (list :operation :hedge :reason :unsafe)
                    (list (list #'cl-resilience-kit:hedge-unsafe-reason
                                :unsafe)))
              (list 'cl-resilience-kit:hedge-exhausted
                    (list :operation :hedge :causes cause :attempts 2)
                    (list (list #'cl-resilience-kit:hedge-exhausted-causes
                                cause)
                          (list #'cl-resilience-kit:hedge-exhausted-attempts
                                2)))
              (list 'cl-resilience-kit:idempotency-key-required
                    (list :operation :idempotency)
                    nil)
              (list 'cl-resilience-kit:idempotency-conflict
                    (list :operation :idempotency
                          :key key
                          :existing-value value)
                    (list (list #'cl-resilience-kit:idempotency-conflict-key
                                key)
                          (list #'cl-resilience-kit:idempotency-conflict-existing-value
                                value))))))
      (dolist (specification specifications)
        (destructuring-bind (type initargs fields) specification
          (let ((condition (apply #'make-condition type initargs)))
            (expect (typep condition type) :to-be-truthy)
            (dolist (field fields)
              (destructuring-bind (reader expected) field
                (expect (funcall reader condition) :to-be expected))))))))

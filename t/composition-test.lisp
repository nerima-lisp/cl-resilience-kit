(in-package #:cl-resilience-kit/test)

(describe "resilience composition"
  (it "uses one bulkhead slot for a complete retrying operation"
    (let* ((bulkhead (make-bulkhead :limit 1))
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
       (call-with-resilience
        (lambda ()
          (incf attempts)
          (if (= attempts 1)
              (error "transient")
              :ok))
        :bulkhead bulkhead
        :retry-policy policy)
       :to-be
       :ok)
      (expect attempts :to-be 2)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "applies rate limiting before the breaker on every attempt"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :capacity 1
                     :initial-tokens 1
                     :refill-rate 0
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (test-fixture-sleeper fixture)))
           (breaker (make-circuit-breaker
                     :failure-threshold 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second 1))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore attempt))
                      (typep condition 'simple-error)))))
      (let ((rejected (expect-condition
                       (lambda ()
                         (call-with-resilience
                          (lambda ()
                            (incf attempts)
                            (error "breaker opens"))
                          :retry-policy policy
                          :rate-limiter limiter
                          :circuit-breaker breaker
                          :clock (test-fixture-clock fixture)
                          :monotonic-units-per-second
                          +test-monotonic-units-per-second+
                          :sleeper (test-fixture-sleeper fixture)))
                       'rate-limit-exceeded)))
        (expect (typep rejected 'rate-limit-exceeded) :to-be-truthy))
      (expect attempts :to-be 1)
      (expect (circuit-breaker-state breaker) :to-be :open)))

  (it "fails closed for an operation without an explicit retry policy"
    (let ((attempts 0)
          (caught nil))
      (handler-case
          (call-with-resilience
           (lambda ()
             (incf attempts)
             (error "non-idempotent failure")))
        (error (condition)
          (setf caught condition)))
      (expect attempts :to-be 1)
      (expect (typep caught 'error) :to-be-truthy)))

  (it "allows a rejected rate result to reach a result classifier"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :capacity 1
                     :initial-tokens 0
                     :refill-rate 0
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (test-fixture-sleeper fixture)))
           (policy (make-retry-policy
                    :max-attempts 1
                    :retry-safe-p t
                    :result-classifier
                    (lambda (result attempt)
                      (declare (ignore result attempt))
                      nil))))
      (multiple-value-bind (result retry-after)
          (call-with-resilience
           (lambda () :operation-never-called)
           :retry-policy policy
           :rate-limiter limiter
           :rate-limit-signal-on-reject-p nil
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second
           +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (expect result :to-be nil)
        (expect retry-after :to-be nil))))

  (it "propagates cancellation, events, and fallback through composition"
    (let* ((fixture (make-test-fixture))
           (token (make-cancellation-token))
           (events nil)
           (bulkhead (make-bulkhead :limit 1))
           (policy (make-retry-policy
                    :max-attempts 3
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-resilience
        (lambda ()
          (cancel-cancellation-token token :user-aborted)
          :ignored)
        :retry-policy policy
        :bulkhead bulkhead
        :cancellation-token token
        :fallback (lambda (condition)
                    (expect (typep condition 'resilience-cancelled)
                            :to-be-truthy)
                    :fallback)
        :event-handler (lambda (event)
                         (push (resilience-event-type event) events))
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second
        +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :fallback)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)
      (expect (member :cancelled events) :to-be-truthy)
      (expect (member :fallback events) :to-be-truthy)))

  (it "keeps caller wait timeout separate from worker hard timeout"
    (let* ((executor (make-resilience-executor :size 1))
           (caught nil))
      (unwind-protect
           (setf caught
                 (expect-condition
                  (lambda ()
                    (resilience-executor-call
                     executor
                     (lambda () (sleep 0.1d0))
                     :hard-timeout 1d0
                     :timeout 0d0
                     :operation :executor-wait))
                  'resilience-execution-timeout))
        (resilience-executor-shutdown executor :wait t))
      (expect (resilience-execution-timeout-timeout caught) :to-be 0d0)
      (expect (resilience-execution-timeout-backend caught)
              :to-be
              :executor-wait)))

  (it "uses the injected clock for hard-timeout observations"
    (let* ((fixture (make-test-fixture))
           (caught
             (expect-condition
              (lambda ()
                (call-with-resilience
                 (lambda () :never-reached)
                 :hard-timeout 0d0
                 :operation :clock-test
                 :clock (test-fixture-clock fixture)
                 :monotonic-units-per-second
                 +test-monotonic-units-per-second+))
              'resilience-hard-timeout)))
      (expect (deadline-exceeded-observed-at caught) :to-be 0d0)
      (expect (resilience-hard-timeout-backend caught) :to-be :thread)))

  (it "reports a worker hard timeout from an executor backend"
    (let* ((executor (make-resilience-executor :size 1))
           (caught nil))
      (unwind-protect
           (setf caught
                 (expect-condition
                  (lambda ()
                    (resilience-executor-call
                     executor
                     (lambda ()
                       (sleep 0.05d0)
                       :never)
                     :hard-timeout 0.01d0
                     :timeout 1d0
                     :operation :worker-timeout))
                  'resilience-hard-timeout))
        (resilience-executor-shutdown executor :wait t))
      (expect (attempt-timeout-timeout caught) :to-be 0.01d0)
      (expect (resilience-hard-timeout-backend caught) :to-be :executor)))

  (it "launches a delayed hedge after the first attempt times out"
    (let ((executor (make-resilience-executor :size 2)))
      (unwind-protect
           (expect
            (call-with-hedging
             (lambda ()
               (sleep 0.05d0)
               :ok)
             :hedge-after 0.01d0
             :max-attempts 2
             :executor executor
             :hedge-safe-p t
             :operation :delayed-hedge)
            :to-be
            :ok)
        (resilience-executor-shutdown executor :wait t))))

  (it "uses the supplied executor for a single hedge attempt"
    (let* ((executor (make-resilience-executor :size 1))
           (caller-thread (current-thread))
           (worker-thread nil)
           (result nil))
      (unwind-protect
           (setf result
                 (call-with-hedging
                  (lambda ()
                    (setf worker-thread (current-thread))
                    :ok)
                  :max-attempts 1
                  :executor executor
                  :hedge-safe-p t
                  :operation :single-hedge))
        (resilience-executor-shutdown executor :wait t))
      (expect result :to-be :ok)
      (expect worker-thread :to-be-truthy)
      (expect (eq caller-thread worker-thread) :to-be nil)))

  (it "shares in-flight coalesced requests and cleans up their entries"
    (let* ((coalescer (make-request-coalescer))
           (started (make-semaphore))
           (release (make-semaphore))
           (calls 0)
           (follower-calls 0)
           (owner-result nil)
           (owner-error nil)
           (owner-thread
             (make-thread
              (lambda ()
                (unwind-protect
                    (handler-case
                        (setf owner-result
                              (multiple-value-list
                               (call-with-request-coalescing
                                coalescer
                                (lambda ()
                                  (incf calls)
                                  (signal-semaphore started)
                                  (wait-on-semaphore release)
                                  (values :ok :second))
                                :key "shared"
                                :idempotency-fingerprint "v1")))
                      (condition (condition)
                        (setf owner-error condition)))
                  (values))
                :owner-done))))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore started :timeout 5) :to-be-truthy)
             (expect (request-coalescer-size coalescer) :to-be 1)
             (expect-condition
              (lambda ()
                (call-with-request-coalescing
                 coalescer
                 (lambda () (error "fingerprint-conflict-must-not-run"))
                 :key "shared"
                 :idempotency-fingerprint "v2"
                 :operation :conflict))
              'idempotency-conflict)
             (let ((caught
                     (expect-condition
                      (lambda ()
                        (call-with-request-coalescing
                         coalescer
                         (lambda ()
                           (incf follower-calls)
                           :wrong)
                         :key "shared"
                         :idempotency-fingerprint "v1"
                         :timeout 0d0
                         :operation :coalesced-wait))
                      'resilience-execution-timeout)))
               (expect (resilience-execution-timeout-backend caught)
                       :to-be
                       :coalescer-wait))
             (signal-semaphore release)
             (expect (join-thread owner-thread
                                  :default :not-joined
                                  :timeout 5)
                     :to-be
                     :owner-done)
             (expect owner-error :to-be nil)
             (expect owner-result :to-equal '(:ok :second))
             (expect calls :to-be 1)
             (expect follower-calls :to-be 0)
             (expect (request-coalescer-size coalescer) :to-be 0))
        (signal-semaphore release)
        (join-thread owner-thread :default :not-joined :timeout 5))))

  (it "requires an idempotency key for coalescing"
    (let ((coalescer (make-request-coalescer)))
      (expect-condition
       (lambda ()
         (call-with-request-coalescing
          coalescer
          (lambda () :must-not-run)))
       'cl-resilience-kit:idempotency-key-required)
      (expect (request-coalescer-size coalescer) :to-be 0)))

  (it "delivers worker failures to coalesced callers"
    (let ((coalescer (make-request-coalescer)))
      (expect-condition
       (lambda ()
         (call-with-request-coalescing
          coalescer
          (lambda () (error "worker failure"))
          :key "worker-error"
          :operation :worker-error))
       'error)
      (expect (request-coalescer-size coalescer) :to-be 0)))

  (it "preserves executor values and rejects submissions after shutdown"
    (let ((executor (make-resilience-executor :size 1)))
      (unwind-protect
           (progn
             (multiple-value-bind (first second)
                 (resilience-executor-call
                  executor
                  (lambda () (values :first :second))
                  :operation :multiple-values)
               (expect first :to-be :first)
               (expect second :to-be :second))
             (expect
              (resilience-executor-call
               executor
               (lambda () :single)
               :operation :single-value)
              :to-be
              :single)
             (resilience-executor-shutdown
              executor
              :wait t
              :timeout 1d0)
             (let ((caught
                     (expect-condition
                      (lambda ()
                        (cl-resilience-kit:resilience-executor-submit
                         executor
                         (lambda () :not-run)
                         :operation :rejected))
                      'cl-resilience-kit:resilience-execution-rejected)))
               (expect
                (cl-resilience-kit:resilience-execution-rejected-reason caught)
                :to-be
                :executor-rejected)
               (expect
                (cl-resilience-kit:resilience-execution-rejected-queue-size
                 caught)
                :to-be
                0)))
        (unless (cl-resilience-kit:resilience-executor-shutdown-p executor)
          (resilience-executor-shutdown executor :wait t)))))

  (it "removes coalesced entries when executor submission is rejected"
    (let* ((executor (make-resilience-executor :size 1))
           (coalescer (make-request-coalescer)))
      (unwind-protect
           (progn
             (resilience-executor-shutdown executor :wait t)
             (let ((caught
                     (expect-condition
                      (lambda ()
                        (call-with-request-coalescing
                         coalescer
                         (lambda () :must-not-run)
                         :executor executor
                         :key "rejected-submission"
                         :operation :rejected-submission))
                      'cl-resilience-kit:resilience-execution-rejected)))
               (expect
                (cl-resilience-kit:resilience-execution-rejected-reason caught)
                :to-be
                :executor-rejected))
             (expect (request-coalescer-size coalescer) :to-be 0))
        (unless (cl-resilience-kit:resilience-executor-shutdown-p executor)
          (resilience-executor-shutdown executor :wait t)))))

  (it "settles and removes a coalesced entry after a nonlocal exit"
    (let* ((coalescer (make-request-coalescer))
           (tag (gensym "COALESCED-EXIT-"))
           (caught
             (expect-condition
              (lambda ()
                (call-with-request-coalescing
                 coalescer
                 (lambda () (throw tag :escaped))
                 :key "nonlocal-exit"
                 :operation :nonlocal-exit))
              'resilience-error)))
      (expect (typep caught 'resilience-error) :to-be-truthy)
      (expect (request-coalescer-size coalescer) :to-be 0))))

(describe "context-derived coalescing identity"
  (it "uses the idempotency key from the current context"
    (let* ((coalescer (make-request-coalescer))
           (executor (make-resilience-executor :size 1)))
      (unwind-protect
           (expect
            (cl-resilience-kit:with-resilience-context
                (:idempotency-key "context-key")
              (call-with-request-coalescing
               coalescer
               (lambda () :ok)
               :executor
               executor
               :operation
               :context-key))
            :to-be
            :ok)
        (cl-resilience-kit:resilience-executor-shutdown
         executor
         :wait
         t))))

  (it "merges explicit context over the current operation context"
    (cl-resilience-kit:with-resilience-context
        (:trace-id "parent-trace")
      (expect
       (call-with-resilience
        (lambda ()
          (let ((context (cl-resilience-kit:current-resilience-context)))
            (list (cl-resilience-kit:resilience-context-operation context)
                  (cl-resilience-kit:resilience-context-trace-id context)
                  (cl-resilience-kit:resilience-context-correlation-id context))))
        :operation :explicit-operation
        :context
        (cl-resilience-kit:make-resilience-context
         :correlation-id
         "child-correlation"))
       :to-equal
       '(:explicit-operation "parent-trace" "child-correlation")))))

(describe "backend timeout propagation"
  (it "propagates a non-await timeout from an executor worker"
    (let ((executor (make-resilience-executor :size 1)))
      (unwind-protect
           (let ((caught
                   (expect-condition
                    (lambda ()
                      (resilience-executor-call
                       executor
                       (lambda ()
                         (error
                          (make-condition
                           'cl-concurrent-kit:operation-timed-out
                           :operation :worker
                           :timeout 0.01d0)))
                       :timeout 1d0
                       :operation :raw-worker-timeout))
                    'cl-concurrent-kit:operation-timed-out)))
             (expect
              (cl-concurrent-kit:operation-timed-out-operation caught)
              :to-be
              :worker))
        (resilience-executor-shutdown executor :wait t))))

  (it "preserves a worker timeout raised inside a hard-timeout wrapper"
    (let ((executor (make-resilience-executor :size 1)))
      (unwind-protect
           (let ((caught
                   (expect-condition
                    (lambda ()
                      (resilience-executor-call
                       executor
                       (lambda ()
                         (error
                          (make-condition
                           'cl-concurrent-kit:operation-timed-out
                           :operation :worker-hard-timeout
                           :timeout 0.01d0)))
                       :hard-timeout 1d0
                       :timeout 2d0
                       :operation :raw-hard-timeout))
                    'cl-concurrent-kit:operation-timed-out)))
             (expect
              (cl-concurrent-kit:operation-timed-out-operation caught)
              :to-be
              :worker-hard-timeout))
        (resilience-executor-shutdown executor :wait t))))

  (it "propagates a non-await timeout from a hedge attempt"
    (let ((caught
            (expect-condition
             (lambda ()
               (call-with-hedging
                (lambda ()
                  (error
                   (make-condition
                    'cl-concurrent-kit:operation-timed-out
                    :operation :hedge-worker
                    :timeout 0.01d0)))
                :hedge-after 0.01d0
                :max-attempts 2
                :hedge-safe-p t
                :operation :raw-hedge-timeout))
             'cl-concurrent-kit:operation-timed-out)))
      (expect
       (cl-concurrent-kit:operation-timed-out-operation caught)
       :to-be
       :hedge-worker))))

  (it "records an initial delayed hedge failure before exhaustion"
    (let ((caught
            (expect-condition
             (lambda ()
               (call-with-hedging
                (lambda ()
                  (error "delayed hedge failure"))
                :hedge-after 0.01d0
                :max-attempts 2
                :hedge-safe-p t
                :operation :delayed-failure))
             'cl-resilience-kit:hedge-exhausted)))
      (expect (cl-resilience-kit:hedge-exhausted-attempts caught) :to-be 2)
      (expect (length (cl-resilience-kit:hedge-exhausted-causes caught)) :to-be 3)))

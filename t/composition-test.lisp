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

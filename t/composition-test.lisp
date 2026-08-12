(in-package #:resilience-kit/test)

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
    (let* ((attempts 0)
           (caught (expect-condition
                    (lambda ()
                      (call-with-resilience
                       (lambda ()
                         (incf attempts)
                         (error "non-idempotent failure"))))
                    'error)))
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
           (release (make-semaphore :count 0))
           (caught nil))
      (unwind-protect
           (setf caught
                 (expect-condition
                  (lambda ()
                    (resilience-executor-call
                     executor
                     (lambda ()
                       (wait-on-semaphore release :timeout 5)
                       :released)
                     :hard-timeout 1d0
                     :timeout 0d0
                     :operation :executor-wait))
                  'resilience-execution-timeout))
        (signal-semaphore release)
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
           (release (make-semaphore :count 0))
           (caught nil))
      (unwind-protect
           (setf caught
                 (expect-condition
                  (lambda ()
                    (resilience-executor-call
                     executor
                     (lambda ()
                       (wait-on-semaphore release :timeout 5)
                       :never)
                     :hard-timeout 0.01d0
                     :timeout 1d0
                     :operation :worker-timeout))
                  'resilience-hard-timeout))
        (signal-semaphore release)
        (resilience-executor-shutdown executor :wait t))
      (expect (attempt-timeout-timeout caught) :to-be 0.01d0)
      (expect (resilience-hard-timeout-backend caught) :to-be :executor)))

  (it "launches a delayed hedge after the first attempt times out"
    (with-test-executor (executor :size 2)
      (let ((started (make-semaphore :count 0))
            (release (make-semaphore :count 0))
            (attempts 0))
        (unwind-protect
             (progn
               (expect
                (call-with-hedging
                 (lambda ()
                   (let ((attempt (incf attempts)))
                     (if (= attempt 1)
                         (progn
                           (signal-semaphore started)
                           (or (wait-on-semaphore release :timeout 5)
                               (error "timed out waiting to release first hedge attempt"))
                           :slow)
                         :ok)))
                 :hedge-after 0.01d0
                 :max-attempts 2
                 :executor executor
                 :hedge-safe-p t
                 :operation :delayed-hedge)
                :to-be
                :ok)
               (expect (wait-on-semaphore started :timeout 5) :to-be-truthy)
               (expect attempts :to-be 2))
          (signal-semaphore release)))))

  (it "uses the supplied executor for a single hedge attempt"
    (let* ((caller-thread (current-thread))
           (worker-thread nil)
           (result nil))
      (with-test-executor (executor :size 1)
        (setf result
              (call-with-hedging
               (lambda ()
                 (setf worker-thread (current-thread))
                 :ok)
               :max-attempts 1
               :executor executor
               :hedge-safe-p t
                :operation :single-hedge)))
      (expect result :to-be :ok)
      (expect worker-thread :to-be-truthy)
      (expect (eq caller-thread worker-thread) :to-be nil))))

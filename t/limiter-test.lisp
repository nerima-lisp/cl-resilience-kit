(in-package #:cl-resilience-kit/test)

(describe "bulkheads and rate limiters"
  (it "rejects a bulkhead call while its only slot is occupied"
    (let* ((bulkhead (make-bulkhead :limit 1))
           (entered (make-semaphore :count 0))
           (release (make-semaphore :count 0))
           (thread (make-thread
                    (lambda ()
                      (bulkhead-call
                       bulkhead
                       (lambda ()
                         (signal-semaphore entered)
                         (wait-on-semaphore release :timeout 5)
                         :held))))))
      (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
      (expect (bulkhead-in-flight bulkhead) :to-be 1)
      (let ((rejected (expect-condition
                       (lambda ()
                         (bulkhead-call bulkhead (lambda () :not-run)))
                       'bulkhead-rejected)))
        (expect (typep rejected 'bulkhead-rejected) :to-be-truthy)
        (expect (bulkhead-rejected-in-flight rejected) :to-be 1))
      (signal-semaphore release)
      (expect (join-thread thread :timeout 5) :to-be :held)
      (expect (bulkhead-limit bulkhead) :to-be 1)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "releases a bulkhead slot when the operation signals"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (expect-condition
       (lambda ()
         (bulkhead-call bulkhead (lambda () (error "failed"))))
       'error)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "observes cancellation after return and releases the bulkhead slot"
    (let* ((token (make-cancellation-token))
           (bulkhead (make-bulkhead :limit 1)))
      (expect-condition
       (lambda ()
         (bulkhead-call
          bulkhead
          (lambda ()
            (cancel-cancellation-token token :completed-elsewhere)
            :returned)
          :cancellation-token token))
       'resilience-cancelled)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)
      (expect (bulkhead-call bulkhead (lambda () :available))
              :to-be
              :available)))

  (it "releases a bulkhead slot when admission observation exits nonlocally"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (expect
       (catch :observer-exit
         (bulkhead-call
          bulkhead
          (lambda () :not-reached)
          :event-handler
          (lambda (event)
            (declare (ignore event))
            (throw :observer-exit :escaped))))
       :to-be :escaped)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)
      (expect (bulkhead-call bulkhead (lambda () :available))
              :to-be :available)))

  (it "refills a token bucket according to fake elapsed time"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :capacity 2
                     :refill-rate 1
                     :initial-tokens 2
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (test-fixture-sleeper fixture))))
      (expect (rate-limiter-acquire limiter :tokens 2) :to-be-truthy)
      (multiple-value-bind (acquired-p retry-after)
          (rate-limiter-acquire limiter)
        (expect acquired-p :to-be nil)
        (expect retry-after :to-be 1.0d0))
      (advance-fixture fixture 1)
      (expect (rate-limiter-acquire limiter) :to-be-truthy)
      (expect (approximately-equal-p (rate-limiter-tokens limiter) 0d0)
              :to-be-truthy)))

  (it "waits through an injected sleeper without real sleep"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :capacity 1
                     :refill-rate 1
                     :initial-tokens 0
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (test-fixture-sleeper fixture))))
      (expect (rate-limiter-acquire limiter :wait-p t) :to-be-truthy)
      (expect (fixture-sleeps fixture) :to-equal '(1.0d0))
      (expect (approximately-equal-p (rate-limiter-tokens limiter) 0d0)
              :to-be-truthy)))

  (it "signals a structured rejection when requested"
    (let* ((fixture (make-test-fixture))
           (events nil)
           (limiter (make-rate-limiter
                     :capacity 1
                     :initial-tokens 0
                     :refill-rate 0
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (let ((rejected (expect-condition
                       (lambda ()
                         (rate-limiter-acquire
                          limiter
                          :signal-on-reject-p t
                          :operation :read
                          :event-handler (lambda (event)
                                           (push event events))))
                       'rate-limit-exceeded)))
        (expect (typep rejected 'rate-limit-exceeded) :to-be-truthy)
        (expect (rate-limit-exceeded-retry-after rejected) :to-be nil)
        (expect (length events) :to-be 1)
        (expect (resilience-event-p (first events)) :to-be-truthy)
        (expect (resilience-event-type (first events))
                :to-be :rate-limit-rejected)
        (expect (resilience-event-operation (first events)) :to-be :read))))

  (it "does not evaluate a macro body after rejection"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :capacity 1
                     :initial-tokens 0
                     :refill-rate 0
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+))
           (ran 0))
      (with-rate-limiter (limiter :signal-on-reject-p nil)
        (incf ran))
      (expect ran :to-be 0)))

  (it "returns after a sleeper that makes no monotonic progress"
    (let* ((fixture (make-test-fixture))
           (sleeps nil)
           (limiter (make-rate-limiter
                     :capacity 1
                     :initial-tokens 0
                     :refill-rate 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (cl-boundary-kit:make-sleeper
                               :sleep-fn (lambda (seconds)
                                           (push seconds sleeps))))))
      (multiple-value-bind (acquired-p retry-after)
          (rate-limiter-acquire limiter :wait-p t)
        (expect acquired-p :to-be nil)
        (expect retry-after :to-be 1.0d0)
        (expect sleeps :to-equal '(1.0d0)))))

  (it "cancels a waiting acquisition through its cooperative token"
    (let* ((fixture (make-test-fixture))
           (token (make-cancellation-token))
           (limiter (make-rate-limiter
                     :capacity 1
                     :initial-tokens 0
                     :refill-rate 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (cl-boundary-kit:make-sleeper
                               :sleep-fn (lambda (seconds)
                                           (declare (ignore seconds))
                                           (cancel-cancellation-token
                                            token
                                            :stop-waiting)))))
           (caught (expect-condition
                    (lambda ()
                      (rate-limiter-acquire
                       limiter
                       :wait-p t
                       :cancellation-token token))
                    'resilience-cancelled)))
      (expect (resilience-cancelled-reason caught) :to-be :stop-waiting))))

  (it "admits a bounded queued caller and releases both slots"
    (let* ((bulkhead (cl-resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 1))
           (entered (make-semaphore :count 0))
           (release (make-semaphore :count 0))
           (holder (make-thread
                    (lambda ()
                      (cl-resilience-kit:queued-bulkhead-call
                       bulkhead
                       (lambda ()
                         (signal-semaphore entered)
                         (wait-on-semaphore release :timeout 5)
                         :held)))))
           (waiter nil)
           (waiter-queued-p nil))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
             (setf waiter
                   (make-thread
                    (lambda ()
                      (cl-resilience-kit:queued-bulkhead-call
                       bulkhead (lambda () :queued)))))
             (loop repeat 500
                   until (= (cl-resilience-kit:queued-bulkhead-queue-size
                              bulkhead)
                            1)
                   do (sleep 0.001))
             (setf waiter-queued-p
                   (= (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                      1))
             (expect waiter-queued-p :to-be-truthy)
             (signal-semaphore release)
             (expect (join-thread holder :timeout 5) :to-be :held)
             (expect (join-thread waiter :timeout 5) :to-be :queued)
             (expect (bulkhead-in-flight bulkhead) :to-be 0)
             (expect (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                     :to-be 0))
        (signal-semaphore release)
        (join-thread holder :timeout 5)
        (when waiter
          (join-thread waiter :timeout 5)))))

  (it "rejects a full queued bulkhead with a structured reason"
    (let* ((bulkhead (cl-resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 0))
           (entered (make-semaphore :count 0))
           (release (make-semaphore :count 0))
           (holder (make-thread
                    (lambda ()
                      (cl-resilience-kit:queued-bulkhead-call
                       bulkhead
                       (lambda ()
                         (signal-semaphore entered)
                         (wait-on-semaphore release :timeout 5)
                         :held))))))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
             (let ((rejected
                     (expect-condition
                      (lambda ()
                        (cl-resilience-kit:queued-bulkhead-call
                         bulkhead (lambda () :not-run)))
                      'cl-resilience-kit:resilience-execution-rejected)))
               (expect
                (cl-resilience-kit:resilience-execution-rejected-reason
                 rejected)
                       :to-be :queue-full)
               (expect
                (cl-resilience-kit:resilience-execution-rejected-queue-size
                 rejected)
                       :to-be 0))
             (signal-semaphore release)
             (expect (join-thread holder :timeout 5) :to-be :held)
             (expect (bulkhead-in-flight bulkhead) :to-be 0))
        (signal-semaphore release)
        (join-thread holder :timeout 5))))

  (it "times out while waiting for a queued bulkhead slot"
    (let* ((bulkhead (cl-resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 1))
           (entered (make-semaphore :count 0))
           (release (make-semaphore :count 0))
           (holder (make-thread
                    (lambda ()
                      (cl-resilience-kit:queued-bulkhead-call
                       bulkhead
                       (lambda ()
                         (signal-semaphore entered)
                         (wait-on-semaphore release :timeout 5)
                         :held))))))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
             (let ((rejected
                     (expect-condition
                      (lambda ()
                        (cl-resilience-kit:queued-bulkhead-call
                         bulkhead (lambda () :not-run) :timeout 0.01))
                      'cl-resilience-kit:resilience-execution-rejected)))
               (expect
                (cl-resilience-kit:resilience-execution-rejected-reason
                 rejected)
                       :to-be :queue-timeout))
             (signal-semaphore release)
             (expect (join-thread holder :timeout 5) :to-be :held)
             (expect (bulkhead-in-flight bulkhead) :to-be 0)
             (expect (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                     :to-be 0))
        (signal-semaphore release)
        (join-thread holder :timeout 5))))

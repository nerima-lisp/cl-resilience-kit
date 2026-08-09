(in-package #:cl-resilience-kit/test)

(describe "bulkheads and rate limiters"
  (it "routes queued bulkheads through the generic bulkhead call"
    (let ((bulkhead
            (cl-resilience-kit:make-queued-bulkhead
             :limit 1
             :max-queue 0)))
      (expect (bulkhead-call bulkhead (lambda () :queued))
              :to-be
              :queued)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

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

  (it "validates rate limiter configuration and exposes its state"
    (expect-condition
     (lambda () (make-rate-limiter :capacity 0))
     'error)
    (expect-condition
     (lambda () (make-rate-limiter :refill-rate -1))
     'error)
    (expect-condition
     (lambda () (make-rate-limiter :capacity 2 :initial-tokens 3))
     'error)
    (let ((limiter
            (make-rate-limiter
             :capacity 2
             :refill-rate 0.5d0
             :initial-tokens 1
             :clock (cl-boundary-kit:make-fake-clock
                     :start 7
                     :monotonic-start 7)
             :monotonic-units-per-second 1)))
      (expect
       (approximately-equal-p
        (cl-resilience-kit:rate-limiter-capacity limiter)
        2d0)
       :to-be-truthy)
      (expect
       (approximately-equal-p
        (cl-resilience-kit:rate-limiter-refill-rate limiter)
        0.5d0)
       :to-be-truthy)
      (expect
       (approximately-equal-p
        (cl-resilience-kit:rate-limiter-last-refill limiter)
        7d0)
       :to-be-truthy)
      (expect (approximately-equal-p (rate-limiter-tokens limiter) 1d0)
              :to-be-truthy)))

  (it "stops rate limiter waits at cooperative deadlines"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :initial-tokens 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (test-fixture-sleeper fixture)))
           (caught
             (expect-condition
              (lambda ()
                (call-with-deadline
                 (lambda ()
                   (advance-fixture fixture 2)
                   (rate-limiter-acquire limiter :operation :expired))
                 :timeout 1d0
                 :clock (test-fixture-clock fixture)
                 :monotonic-units-per-second
                 +test-monotonic-units-per-second+))
              'deadline-exceeded)))
      (expect (cl-resilience-kit:deadline-exceeded-stage caught)
              :to-be
              :rate-limit))
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :initial-tokens 0
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper (test-fixture-sleeper fixture)))
           (caught
             (expect-condition
              (lambda ()
                (call-with-deadline
                 (lambda ()
                   (rate-limiter-acquire
                    limiter
                    :wait-p t
                    :operation :wait-expired))
                 :timeout 0.5d0
                 :clock (test-fixture-clock fixture)
                 :monotonic-units-per-second
                 +test-monotonic-units-per-second+))
              'deadline-exceeded)))
      (expect (cl-resilience-kit:deadline-exceeded-stage caught)
              :to-be
              :rate-limit)))

  (it "checks a cooperative deadline after a rate limiter sleeper"
    (let* ((fixture (make-test-fixture))
           (limiter (make-rate-limiter
                     :initial-tokens 0
                     :refill-rate 4
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+
                     :sleeper
                     (cl-boundary-kit:make-sleeper
                      :sleep-fn
                      (lambda (seconds)
                        (declare (ignore seconds))
                        (advance-fixture fixture 1d0)))))
           (caught
             (expect-condition
              (lambda ()
                (call-with-deadline
                 (lambda ()
                   (rate-limiter-acquire
                    limiter
                    :wait-p t
                    :operation :sleeper-expired))
                 :timeout 1d0
                 :clock (test-fixture-clock fixture)
                 :monotonic-units-per-second
                 +test-monotonic-units-per-second+))
              'deadline-exceeded)))
      (expect (cl-resilience-kit:deadline-exceeded-stage caught)
              :to-be
              :rate-limit)))

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

(describe "bulkhead admission cancellation boundaries"
  (it "checks a live cancellation token around admitted work"
    (let* ((token (make-cancellation-token))
           (bulkhead (cl-resilience-kit:make-queued-bulkhead
                      :limit 1
                      :max-queue 0)))
      (expect
       (cl-resilience-kit:queued-bulkhead-call
        bulkhead
        (lambda () :ok)
        :cancellation-token token
        :operation :live-token)
       :to-be
       :ok)
      (expect (cl-resilience-kit:bulkhead-in-flight bulkhead)
              :to-be
              0))))

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

  (it "honors an ambient deadline while waiting for queued admission"
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
           (releaser nil))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
             (setf releaser
                   (make-thread
                    (lambda ()
                      (sleep 0.01)
                      (signal-semaphore release)
                      :released)))
             (expect
              (call-with-deadline
               (lambda ()
                 (cl-resilience-kit:queued-bulkhead-call
                  bulkhead
                  (lambda () :ambient-deadline)))
               :timeout 1d0)
              :to-be
              :ambient-deadline)
             (expect (join-thread releaser :timeout 5) :to-be :released)
             (expect (join-thread holder :timeout 5) :to-be :held)
             (expect (bulkhead-in-flight bulkhead) :to-be 0)
             (expect (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                     :to-be
                     0))
        (signal-semaphore release)
        (join-thread holder :timeout 5)
        (when releaser
          (join-thread releaser :timeout 5)))))

  (it "uses the earliest ambient or local queue deadline"
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
           (rejected nil))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
             (setf rejected
                   (expect-condition
                    (lambda ()
                      (call-with-deadline
                       (lambda ()
                         (cl-resilience-kit:queued-bulkhead-call
                          bulkhead
                          (lambda () :not-run)
                          :timeout 0d0))
                       :timeout 1d0))
                    'cl-resilience-kit:resilience-execution-rejected))
             (expect
              (cl-resilience-kit:resilience-execution-rejected-reason
               rejected)
              :to-be
              :queue-timeout)
             (signal-semaphore release)
             (expect (join-thread holder :timeout 5) :to-be :held)
             (expect (bulkhead-in-flight bulkhead) :to-be 0)
             (expect (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                     :to-be
                     0))
        (signal-semaphore release)
        (join-thread holder :timeout 5))))

  (it "cancels a queued bulkhead waiter and removes its queue entry"
    (let* ((bulkhead (cl-resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 1))
           (token (make-cancellation-token))
           (entered (make-semaphore :count 0))
           (release (make-semaphore :count 0))
           (waiter-done (make-semaphore :count 0))
           (waiter-condition nil)
           (holder
             (make-thread
              (lambda ()
                (cl-resilience-kit:queued-bulkhead-call
                 bulkhead
                 (lambda ()
                   (signal-semaphore entered)
                   (wait-on-semaphore release :timeout 5)
                   :held)))))
           (waiter nil))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore entered :timeout 5) :to-be-truthy)
             (setf waiter
                   (make-thread
                    (lambda ()
                      (unwind-protect
                           (handler-case
                               (cl-resilience-kit:queued-bulkhead-call
                                bulkhead
                                (lambda () :not-run)
                                :cancellation-token token
                                :operation :cancelled-waiter)
                             (condition (condition)
                               (setf waiter-condition condition)))
                        (signal-semaphore waiter-done)))))
             (loop repeat 500
                   until (= (cl-resilience-kit:queued-bulkhead-queue-size
                              bulkhead)
                            1)
                   do (sleep 0.001))
             (expect (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                     :to-be
                     1)
             (cancel-cancellation-token token :cancelled)
             (expect (wait-on-semaphore waiter-done :timeout 5)
                     :to-be-truthy)
             (expect (typep waiter-condition 'resilience-cancelled)
                     :to-be-truthy)
             (expect (resilience-cancelled-reason waiter-condition)
                     :to-be
                     :cancelled)
             (expect (cl-resilience-kit:queued-bulkhead-queue-size bulkhead)
                     :to-be
                     0)
             (signal-semaphore release)
             (expect (join-thread holder :timeout 5) :to-be :held)
             (expect (join-thread waiter :timeout 5) :to-be-truthy)
             (expect (bulkhead-in-flight bulkhead) :to-be 0))
        (signal-semaphore release)
        (join-thread holder :timeout 5)
        (when waiter
          (join-thread waiter :timeout 5)))))

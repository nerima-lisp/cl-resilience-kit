(in-package #:resilience-kit/test)

(describe "rate limiters"
  (it "rejects non-real rate limiter configuration"
    (expect
     (lambda () (make-rate-limiter :capacity :invalid))
     :to-throw
     'error)
    (expect
     (lambda () (make-rate-limiter :refill-rate :invalid))
     :to-throw
     'error))

  (it "refills a token bucket according to fake elapsed time"
    (with-test-rate-limiter (fixture limiter
                             :capacity 2
                             :refill-rate 1
                             :initial-tokens 2)
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
    (with-test-rate-limiter (fixture limiter
                             :capacity 1
                             :refill-rate 1
                             :initial-tokens 0)
      (expect (rate-limiter-acquire limiter :wait-p t) :to-be-truthy)
      (expect (fixture-sleeps fixture) :to-equal '(1.0d0))
      (expect (approximately-equal-p (rate-limiter-tokens limiter) 0d0)
              :to-be-truthy)))

  (it "signals a structured rejection when requested"
    (let ((events nil))
      (with-test-rate-limiter (fixture limiter
                               :capacity 1
                               :initial-tokens 0
                               :refill-rate 0)
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
          (expect (resilience-event-operation (first events)) :to-be :read)))))

  (it "does not evaluate a macro body after rejection"
    (let ((ran 0))
      (with-test-rate-limiter (fixture limiter
                               :capacity 1
                               :initial-tokens 0
                               :refill-rate 0)
        (with-rate-limiter (limiter :signal-on-reject-p nil)
          (incf ran))
        (expect ran :to-be 0))))

  (it "returns after a sleeper that makes no monotonic progress"
    (let ((sleeps nil))
      (with-test-rate-limiter (fixture limiter
                               :capacity 1
                               :initial-tokens 0
                               :refill-rate 1
                               :sleeper (make-sleeper
                                         :sleep-fn (lambda (seconds)
                                                     (push seconds sleeps))))
      (multiple-value-bind (acquired-p retry-after)
          (rate-limiter-acquire limiter :wait-p t)
        (expect acquired-p :to-be nil)
        (expect retry-after :to-be 1.0d0)
          (expect sleeps :to-equal '(1.0d0))))))

  (it "cancels a waiting acquisition through its cooperative token"
    (let ((token (make-cancellation-token)))
      (with-test-rate-limiter (fixture limiter
                               :capacity 1
                               :initial-tokens 0
                               :refill-rate 1
                               :sleeper (make-sleeper
                                         :sleep-fn (lambda (seconds)
                                                     (declare (ignore seconds))
                                                     (cancel-cancellation-token
                                                      token
                                                      :stop-waiting))))
        (let ((caught (expect-condition
                       (lambda ()
                         (rate-limiter-acquire
                          limiter
                          :wait-p t
                          :cancellation-token token))
                       'resilience-cancelled)))
          (expect (resilience-cancelled-reason caught) :to-be :stop-waiting)))))

  (it "validates rate limiter configuration and exposes its state"
    (expect
     (lambda () (make-rate-limiter :capacity 0))
     :to-throw
     'error)
    (expect
     (lambda () (make-rate-limiter :refill-rate -1))
     :to-throw
     'error)
    (expect
     (lambda () (make-rate-limiter :capacity 2 :initial-tokens 3))
     :to-throw
     'error)
    (let ((limiter
            (make-rate-limiter
             :capacity 2
             :refill-rate 0.5d0
             :initial-tokens 1
             :clock (make-fake-clock
                     :start 7
                     :monotonic-start 7)
             :monotonic-units-per-second 1)))
      (expect
       (approximately-equal-p
        (rate-limiter-capacity limiter)
        2d0)
       :to-be-truthy)
      (expect
       (approximately-equal-p
        (rate-limiter-refill-rate limiter)
        0.5d0)
       :to-be-truthy)
      (expect
       (approximately-equal-p
        (rate-limiter-last-refill limiter)
        7d0)
       :to-be-truthy)
      (expect (approximately-equal-p (rate-limiter-tokens limiter) 1d0)
              :to-be-truthy)))

  (it "stops rate limiter waits at cooperative deadlines"
    (with-test-rate-limiter (fixture limiter
                             :initial-tokens 1)
      (let ((caught
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
        (expect (deadline-exceeded-stage caught)
                :to-be
                :rate-limit)))
    (with-test-rate-limiter (fixture limiter
                             :initial-tokens 0)
      (let ((caught
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
        (expect (deadline-exceeded-stage caught)
                :to-be
                :rate-limit))))

  (it "checks a cooperative deadline after a rate limiter sleeper"
    (with-test-rate-limiter (fixture limiter
                             :initial-tokens 0
                             :refill-rate 4
                             :sleeper
                             (make-sleeper
                              :sleep-fn
                              (lambda (seconds)
                                (declare (ignore seconds))
                                (advance-fixture fixture 1d0))))
      (let ((caught
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
        (expect (deadline-exceeded-stage caught)
                :to-be
                :rate-limit)))))

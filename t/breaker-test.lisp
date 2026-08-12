(in-package #:resilience-kit/test)

(describe "circuit breaker"
  (it "opens, rejects, then closes after a successful half-open probe"
    (let* ((fixture (make-test-fixture))
           (breaker (make-circuit-breaker
                     :failure-threshold 1
                     :reset-timeout 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect-condition
       (lambda ()
         (circuit-breaker-call breaker
                               (lambda () (error "service failed"))))
       'error)
      (expect (circuit-breaker-state breaker) :to-be :open)
      (expect (circuit-breaker-opened-at breaker) :to-be-truthy)
      (expect (circuit-breaker-generation breaker) :to-be 1)
      (let ((rejected (expect-condition
                       (lambda ()
                         (circuit-breaker-call breaker (lambda () :no)))
                       'resilience-kit:circuit-open)))
        (expect (typep rejected 'resilience-kit:circuit-open)
                :to-be-truthy))
      (advance-fixture fixture 1)
      (expect (circuit-breaker-call breaker (lambda () :ok)) :to-be :ok)
      (expect (circuit-breaker-state breaker) :to-be :closed)
      (expect (circuit-breaker-opened-at breaker) :to-be nil)
      (expect (circuit-breaker-generation breaker) :to-be 3)
      (expect (circuit-breaker-failure-count breaker) :to-be 0)))

  (it "classifies results as failures when configured"
    (let* ((fixture (make-test-fixture))
           (breaker (make-circuit-breaker
                     :failure-threshold 1
                     :result-classifier (lambda (result attempt)
                                          (declare (ignore attempt))
                                          (eq result :failure))
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect (circuit-breaker-call breaker (lambda () :failure))
              :to-be
              :failure)
      (expect (circuit-breaker-state breaker) :to-be :open)))

  (it "reports classifier failures after recording the failure"
    (let ((breaker
            (make-circuit-breaker
             :failure-threshold 1
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               (error "condition classifier failed")))))
      (expect-condition
       (lambda ()
         (circuit-breaker-call
          breaker
          (lambda () (error "operation failed"))))
       'error)
      (expect (circuit-breaker-state breaker) :to-be :open)
      (expect (circuit-breaker-failure-count breaker) :to-be 0)))

  (it "does not count cancellation observed after return as a failure"
    (let* ((token (make-cancellation-token))
           (breaker (make-circuit-breaker :failure-threshold 1)))
      (expect-condition
       (lambda ()
         (circuit-breaker-call
          breaker
          (lambda ()
            (cancel-cancellation-token token :completed-elsewhere)
            :returned)
          :cancellation-token token))
       'resilience-cancelled)
      (expect (circuit-breaker-state breaker) :to-be :closed)
      (expect (circuit-breaker-failure-count breaker) :to-be 0)))

  (it "limits concurrent half-open probes"
    (let* ((fixture (make-test-fixture))
           (breaker (make-circuit-breaker
                     :failure-threshold 1
                     :reset-timeout 1
                     :half-open-probe-limit 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect-condition
       (lambda ()
         (circuit-breaker-call breaker (lambda () (error "open me"))))
       'error)
      (advance-fixture fixture 1)
      (with-blocking-operation-thread
          (entered release :result :ok)
          (circuit-breaker-call
           breaker
           (make-blocking-operation
            entered
            release
            :result :ok))
        (expect (circuit-breaker-active-probes breaker) :to-be 1)
        (let ((rejected (expect-condition
                         (lambda ()
                           (circuit-breaker-call breaker (lambda () :no)))
                         'resilience-kit:circuit-open)))
          (expect (typep rejected 'resilience-kit:circuit-open)
                  :to-be-truthy)))
      (expect (circuit-breaker-state breaker) :to-be :closed)
      (expect (circuit-breaker-active-probes breaker) :to-be 0)))

  (it "can be reset explicitly and invalidates the open state"
    (let* ((fixture (make-test-fixture))
           (breaker (make-circuit-breaker
                     :failure-threshold 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect-condition
       (lambda ()
         (circuit-breaker-call breaker (lambda () (error "open me"))))
       'error)
      (circuit-breaker-reset breaker)
      (expect (circuit-breaker-state breaker) :to-be :closed)
      (expect (circuit-breaker-call breaker (lambda () :ok)) :to-be :ok)))

  (it-concurrent "updates failure state atomically across callers"
    (let* ((fixture (make-test-fixture))
           (breaker (make-circuit-breaker
                     :failure-threshold 1000
                     :condition-classifier (lambda (condition attempt)
                                             (declare (ignore condition attempt))
                                             t)
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+))
           (threads
             (loop repeat 32
                   collect
                   (make-thread
                    (lambda ()
                      (expect-condition
                       (lambda ()
                         (circuit-breaker-call
                          breaker
                          (lambda () (error "concurrent failure"))))
                       'error))))))
      (join-all threads)
      (expect (circuit-breaker-failure-count breaker) :to-be 32)
      (expect (circuit-breaker-state breaker) :to-be :closed)))

  (it "releases a half-open probe after a nonlocal exit"
    (let* ((fixture (make-test-fixture))
           (breaker (make-circuit-breaker
                     :failure-threshold 1
                     :reset-timeout 1
                     :half-open-probe-limit 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect-condition
       (lambda ()
         (circuit-breaker-call breaker (lambda () (error "open me"))))
       'error)
      (advance-fixture fixture 1)
      (expect
       (catch 'probe-aborted
         (circuit-breaker-call
          breaker
          (lambda () (throw 'probe-aborted :aborted))))
       :to-be
       :aborted)
      (expect (circuit-breaker-active-probes breaker) :to-be 0)
      (expect (circuit-breaker-state breaker) :to-be :open)
      (expect-condition
       (lambda ()
         (circuit-breaker-call breaker (lambda () :not-run)))
       'circuit-open))))

(describe "distributed circuit breaker"
  (it "shares state through versioned storage and closes after a probe"
    (let* ((fixture (make-test-fixture))
           (store (resilience-kit:make-memory-state-store))
           (breaker (resilience-kit:make-distributed-circuit-breaker
                     :store store
                     :key "service-a"
                     :failure-threshold 1
                     :reset-timeout 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker
          (lambda () (error "distributed failure"))))
       'error)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be :open)
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () :not-run)))
       'resilience-kit:circuit-open)
      (advance-fixture fixture 1)
      (expect
       (resilience-kit:distributed-circuit-breaker-call
        breaker (lambda () :healthy))
       :to-be
       :healthy)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be :closed)
      (expect (resilience-kit:distributed-circuit-breaker-active-probes
               breaker)
              :to-be 0)))

  (it "releases a distributed probe after a nonlocal exit"
    (let* ((fixture (make-test-fixture))
           (store (resilience-kit:make-memory-state-store))
           (breaker (resilience-kit:make-distributed-circuit-breaker
                     :store store
                     :key "service-b"
                     :failure-threshold 1
                     :reset-timeout 1
                     :clock (test-fixture-clock fixture)
                     :monotonic-units-per-second
                     +test-monotonic-units-per-second+)))
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () (error "open distributed breaker"))))
       'error)
      (advance-fixture fixture 1)
      (expect
       (catch 'distributed-probe-aborted
         (resilience-kit:distributed-circuit-breaker-call
          breaker
          (lambda () (throw 'distributed-probe-aborted :aborted))))
       :to-be
       :aborted)
      (expect (resilience-kit:distributed-circuit-breaker-active-probes
               breaker)
              :to-be 0)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be :open))))

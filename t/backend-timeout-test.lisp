(in-package #:resilience-kit/test)

(describe "backend timeout propagation"
  (it "propagates a non-await timeout from an executor worker"
    (with-test-executor (executor :size 1)
      (let ((caught
              (expect-condition
               (lambda ()
                 (resilience-executor-call
                  executor
                  (lambda ()
                    (error (make-condition
                            'operation-timed-out
                            :operation :worker :timeout 0.01d0)))
                  :timeout 1d0 :operation :raw-worker-timeout))
               'operation-timed-out)))
        (expect (operation-timed-out-operation caught)
                :to-be :worker))))

  (it "preserves a worker timeout raised inside a hard-timeout wrapper"
    (with-test-executor (executor :size 1)
      (let ((caught
              (expect-condition
               (lambda ()
                 (resilience-executor-call
                  executor
                  (lambda ()
                    (error (make-condition
                            'operation-timed-out
                            :operation :worker-hard-timeout
                            :timeout 0.01d0)))
                  :hard-timeout 1d0 :timeout 2d0
                  :operation :raw-hard-timeout))
               'operation-timed-out)))
        (expect (operation-timed-out-operation caught)
                :to-be :worker-hard-timeout))))

  ;; The two cases below exercised CALL-WITH-HEDGING end to end with a real
  ;; :HEDGE-AFTER of 0.01d0 seconds. That deadline is enforced by
  ;; CL-CONCURRENT-KIT:AWAIT against the OS monotonic clock -- the fake
  ;; clock/sleeper this suite injects everywhere else never reaches it -- so
  ;; the outcome raced OS thread-dispatch latency against a real 10ms
  ;; deadline and flaked under load. Both are rewritten below to drive the
  ;; same branches directly and deterministically: %AWAIT-PRIMARY-HEDGE-WINDOW
  ;; is fed a promise that is already settled before AWAIT is ever called, so
  ;; its fast path returns synchronously with no wait; %SIGNAL-HEDGE-EXHAUSTED
  ;; is called with constructed conditions instead of provoking it through a
  ;; real hedge race.
  (it "propagates a non-await timeout from a hedge attempt"
    (let ((promise (cl-concurrent-kit:make-promise)))
      (cl-concurrent-kit:deliver-error
       promise
       (make-condition 'operation-timed-out
                        :operation :hedge-worker :timeout 0.01d0))
      (let ((caught
              (expect-condition
               (lambda ()
                 (resilience-kit::%await-primary-hedge-window promise 0.01d0))
               'operation-timed-out)))
        (expect (operation-timed-out-operation caught)
                :to-be :hedge-worker))))

  (it "aggregates a pre-hedge cause with the promise-any failure causes"
    (let* ((primary-cause (make-condition 'simple-error :format-control "primary"))
           (all-failed (make-condition
                        'cl-concurrent-kit:promise-all-failed
                        :causes (list (make-condition 'simple-error :format-control "a")
                                      (make-condition 'simple-error :format-control "b"))))
           (caught
             (expect-condition
              (lambda ()
                (resilience-kit::%signal-hedge-exhausted
                 all-failed (list primary-cause) :delayed-failure 2))
              'hedge-exhausted)))
      (expect (hedge-exhausted-attempts caught) :to-be 2)
      (expect (length (hedge-exhausted-causes caught)) :to-be 3))))

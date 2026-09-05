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

  ;; Exercise hedge timeout branches directly: real 10ms races are
  ;; scheduler-dependent, while settled promises and constructed conditions
  ;; keep these propagation checks deterministic.
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

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

  (it "propagates a non-await timeout from a hedge attempt"
    (let ((caught
            (expect-condition
             (lambda ()
               (call-with-hedging
                (lambda ()
                  (error (make-condition
                          'operation-timed-out
                          :operation :hedge-worker :timeout 0.01d0)))
                :hedge-after 0.01d0 :max-attempts 2 :hedge-safe-p t
                :operation :raw-hedge-timeout))
             'operation-timed-out)))
      (expect (operation-timed-out-operation caught)
              :to-be :hedge-worker)))

  (it "records an initial delayed hedge failure before exhaustion"
    (let ((caught
            (expect-condition
             (lambda ()
               (call-with-hedging
                (lambda () (error "delayed hedge failure"))
                :hedge-after 0.01d0 :max-attempts 2 :hedge-safe-p t
                :operation :delayed-failure))
             'hedge-exhausted)))
      (expect (hedge-exhausted-attempts caught) :to-be 2)
      (expect (length (hedge-exhausted-causes caught)) :to-be 3))))

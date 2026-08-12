(in-package #:resilience-kit/test)

(describe "retry execution contracts"
  (it "publishes attempt events and tolerates observer failures"
    (with-test-condition-retry-policy
        (fixture
         policy
         :max-attempts 2
         :initial-delay 0)
      (let ((events nil)
            (warnings nil)
            (attempts 0))
        (handler-bind
            ((resilience-kit::resilience-observer-warning
               (lambda (warning)
                 (push warning warnings)
                 (muffle-warning warning))))
          (expect
           (call-with-test-retry
            fixture
            policy
            (lambda ()
              (incf attempts)
              (if (= attempts 1)
                  (error "transient")
                  :ok))
            :operation :read
            :event-handler
            (lambda (event)
              (push event events)
              (when (eq (resilience-event-type event) :attempt-failure)
                (error "observer failure"))))
           :to-be
           :ok))
        (let ((types (mapcar #'resilience-event-type (reverse events))))
          (expect (count :attempt-start types) :to-be 2)
          (expect (count :attempt-failure types) :to-be 1)
          (expect (count :attempt-success types) :to-be 1)
          (expect (count :retry-scheduled types) :to-be 1)
          (expect (every (lambda (event)
                           (and (resilience-event-p event)
                                (eq (resilience-event-operation event) :read)))
                         events)
                  :to-be-truthy))
        (expect (length warnings) :to-be 1)
        (expect
         (eq (resilience-kit::resilience-observer-warning-phase
              (first warnings))
             :event-handler)
         :to-be-truthy))))

  (it "invokes a fallback when retry exhaustion is terminal"
    (with-test-condition-retry-policy
        (fixture
         policy
         :max-attempts 1)
      (let ((fallback-condition nil))
        (expect
         (call-with-test-retry
          fixture
          policy
          (lambda () (error "permanent"))
          :fallback (lambda (condition)
                      (setf fallback-condition condition)
                      :fallback))
         :to-be
         :fallback)
        (expect (typep fallback-condition 'retry-exhausted)
                :to-be-truthy)
        (expect (retry-exhausted-attempts fallback-condition) :to-be 1))))

  (it "surfaces classifier failures as public resilience conditions"
    (with-test-retry-policy
        (fixture
         policy
         :max-attempts 3
         :retry-safe-p t
         :condition-classifier
         (lambda (condition attempt)
           (declare (ignore condition attempt))
           (error "classifier failure")))
      (let* ((attempts 0)
             (caught (expect-condition
                      (lambda ()
                        (call-with-test-retry
                         fixture
                         policy
                         (lambda ()
                           (incf attempts)
                           (error "operation failure"))))
                      'retry-classifier-error)))
        (expect attempts :to-be 1)
        (expect (typep (retry-classifier-error-cause caught) 'error)
                :to-be-truthy))))

  (it "applies numeric result classifier delay hints during execution"
    (with-test-retry-policy
        (fixture
         policy
         :max-attempts 2
         :initial-delay 1
         :retry-safe-p t
         :result-classifier
         (lambda (result attempt)
           (declare (ignore result))
           (when (= attempt 1) 5)))
      (let ((attempts 0))
        (expect
         (call-with-test-retry
          fixture
          policy
          (lambda ()
            (incf attempts)
            (if (= attempts 1) :retryable :ok)))
         :to-be
         :ok)
        (expect (fixture-sleeps fixture) :to-equal '(5.0d0))
        (expect attempts :to-be 2))))

  (it "keeps exponential backoff finite under numeric overflow pressure"
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier most-positive-double-float)))
      (let ((delay (compute-backoff-delay policy 2)))
        (expect (floatp delay) :to-be-truthy)
        (expect (<= 0d0 delay most-positive-double-float)
                :to-be-truthy)))))

  (it "caps very large retry numbers without a linear delay loop"
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier 2
                   :max-delay 10)))
      (expect (compute-backoff-delay policy most-positive-fixnum)
              :to-be
              10d0)))

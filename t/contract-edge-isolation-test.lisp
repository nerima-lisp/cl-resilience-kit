(in-package #:resilience-kit/test)

(describe "isolation and macro edge contracts"
  (it "completes local half-open probes and classifier failures"
    (expect-invalid-calls (#'resilience-kit:make-circuit-breaker)
      (:failure-threshold 0)
      (:condition-classifier 1))
    (let* ((fixture (make-test-fixture :start 0))
           (breaker
             (resilience-kit:make-circuit-breaker
              :failure-threshold 1
              :reset-timeout 2
              :half-open-probe-limit 1
              :success-threshold 2
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect (resilience-kit:circuit-breaker-failure-threshold breaker)
              :to-be
              1)
      (expect (approximately-equal-p
               (resilience-kit:circuit-breaker-reset-timeout breaker)
               2)
              :to-be-truthy)
      (expect (resilience-kit:circuit-breaker-half-open-probe-limit breaker)
              :to-be
              1)
      (expect (resilience-kit:circuit-breaker-success-threshold breaker)
              :to-be
              2)
      (expect-condition
       (lambda ()
         (resilience-kit:circuit-breaker-call
          breaker (lambda () (error "local failure"))))
       'error)
      (expect (resilience-kit:circuit-breaker-state breaker)
              :to-be
              :open)
      (advance-fixture fixture 2)
      (expect (resilience-kit:circuit-breaker-call
               breaker (lambda () :probe-one))
              :to-be
              :probe-one)
      (expect (resilience-kit:circuit-breaker-state breaker)
              :to-be
              :half-open)
      (expect (resilience-kit:circuit-breaker-call
               breaker (lambda () :probe-two))
              :to-be
              :probe-two)
      (expect (resilience-kit:circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:with-circuit-breaker
                   (breaker :operation :macro)
                 :macro-result)
              :to-be
              :macro-result))
    (let ((breaker
            (resilience-kit:make-circuit-breaker
             :failure-threshold 1
             :result-classifier
             (lambda (value attempt)
               (declare (ignore value attempt))
               (error "result classifier failure")))))
      (expect-condition
       (lambda ()
         (resilience-kit:circuit-breaker-call
          breaker (lambda () :ok)))
       'error)
      (expect (resilience-kit:circuit-breaker-state breaker)
              :to-be
              :open))
    (let ((breaker
            (resilience-kit:make-circuit-breaker
             :failure-threshold 1
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               nil))))
      (expect-condition
       (lambda ()
         (resilience-kit:circuit-breaker-call
          breaker (lambda () (error "ignored local failure"))))
       'error)
      (expect (resilience-kit:circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:circuit-breaker-failure-count breaker)
              :to-be
              0)))

  (it "rejects invalid request-coalescing macro options at expansion time"
    (expect-invalid-macroexpansions ()
      (resilience-kit:with-request-coalescing
          (coalescer :key "once" :key "twice")
        :unreachable)
      (resilience-kit:with-request-coalescing
          (coalescer :not-an-option t)
        :unreachable)
      (resilience-kit:with-request-coalescing
          (coalescer :key)
        :unreachable)
      (resilience-kit:with-request-coalescing
          (coalescer "not-a-keyword" "value")
        :unreachable))
    (let ((cyclic-options (list :key "value")))
      (setf (cddr cyclic-options) cyclic-options)
      (expect-condition-form
       (resilience-kit::%validate-request-coalescing-options cyclic-options)
       'error)))

  (it "preserves direct macro-to-function expansions"
    (expect-macroexpansions
      ((resilience-kit:with-retry (policy :operation :retry) :ok)
       (resilience-kit:call-with-retry
        policy
        (lambda () :ok)
        :operation
        :retry))
      ((resilience-kit:with-deadline (:timeout 1d0) :ok)
       (resilience-kit:call-with-deadline
        (lambda () :ok)
        :timeout
        1d0))
      ((resilience-kit:with-bulkhead (bulkhead :operation :bulkhead) :ok)
       (resilience-kit:bulkhead-call
        bulkhead
        (lambda () :ok)
        :operation
        :bulkhead
        :cancellation-token
        nil
        :event-handler
        nil
        :timeout
        nil))
      ((resilience-kit:with-circuit-breaker (breaker :operation :breaker) :ok)
       (resilience-kit:circuit-breaker-call
        breaker
        (lambda () :ok)
        :operation
        :breaker))
      ((resilience-kit:with-distributed-circuit-breaker
           (breaker :operation :distributed)
         :ok)
       (resilience-kit:distributed-circuit-breaker-call
        breaker
        (lambda () :ok)
        :operation
        :distributed))
      ((resilience-kit:with-resilience-executor
           (executor :operation :executor)
         :ok)
       (resilience-kit:resilience-executor-call
        executor
        (lambda () :ok)
        :operation
        :executor))
      ((resilience-kit:with-hedging (:operation :hedge) :ok)
       (resilience-kit:call-with-hedging
        (lambda () :ok)
        :operation
        :hedge)))))

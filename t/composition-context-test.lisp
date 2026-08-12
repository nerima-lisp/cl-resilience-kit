(in-package #:resilience-kit/test)

(describe "context-derived coalescing identity"
  (it "uses the idempotency key from the current context"
    (let ((coalescer (make-request-coalescer)))
      (with-test-executor (executor :size 1)
        (expect
         (resilience-kit:with-resilience-context
             (:idempotency-key "context-key")
           (call-with-request-coalescing
            coalescer (lambda () :ok)
            :executor executor :operation :context-key))
         :to-be :ok))))

  (it "merges explicit context over the current operation context"
    (resilience-kit:with-resilience-context
        (:trace-id "parent-trace")
      (expect
       (call-with-resilience
        (lambda ()
          (let ((context (resilience-kit:current-resilience-context)))
            (list (resilience-kit:resilience-context-operation context)
                  (resilience-kit:resilience-context-trace-id context)
                  (resilience-kit:resilience-context-correlation-id context))))
        :operation :explicit-operation
       :context (resilience-kit:make-resilience-context
                  :correlation-id "child-correlation"))
       :to-equal '(:explicit-operation "parent-trace" "child-correlation")))))

  (describe "inherited resilience event handlers"
    (it "evaluates the handler once and forwards operation events"
      (let ((evaluations 0)
            (event-types nil))
      (expect
       (resilience-kit:with-resilience-event-handler
           ((progn
              (incf evaluations)
              (lambda (event)
                (push (resilience-event-type event) event-types))))
         (call-with-resilience (lambda () :ok)
                               :operation :inherited-handler))
       :to-be :ok)
        (expect evaluations :to-be 1)
        (expect (member :operation-complete event-types) :to-be-truthy)))

    (it "preserves the operation failure when the inherited handler fails"
      (let ((warnings nil)
            (condition nil))
        (handler-bind
            ((resilience-kit::resilience-observer-warning
               (lambda (warning)
                 (push warning warnings)
                 (muffle-warning warning))))
          (resilience-kit:with-resilience-event-handler
              ((lambda (event)
                 (when (eq (resilience-event-type event) :operation-failed)
                   (error "observer failure"))))
            (setf condition
                  (expect-condition
                   (lambda ()
                     (call-with-resilience
                      (lambda ()
                        (error 'simple-error
                               :format-control "operation failure"))
                      :operation :failing-handler))
                   'simple-error))))
        (expect (typep condition 'simple-error) :to-be-truthy)
        (expect (length warnings) :to-be 1)
        (expect
         (eq (resilience-kit::resilience-observer-warning-phase
              (first warnings))
             :event-handler)
         :to-be-truthy)))

    (it "allows an inner binding to disable an inherited handler"
      (let ((events nil))
        (resilience-kit:with-resilience-event-handler
            ((lambda (event)
             (push event events)))
        (resilience-kit:with-resilience-event-handler (nil)
          (call-with-resilience (lambda () :ok)
                                :operation :disabled-handler)))
      (expect events :to-equal nil))))

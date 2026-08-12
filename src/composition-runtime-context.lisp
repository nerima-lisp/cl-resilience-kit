(in-package #:resilience-kit)

(defun %plan-active-context (plan)
  "Build the dynamic context for PLAN without executing its operation."
  (let ((base-context
          (merge-resilience-context
           (current-resilience-context)
           (make-resilience-context
            :operation (resilience-plan-operation plan)
            :idempotency-key (resilience-plan-idempotency-key plan)))))
    (if (resilience-plan-context plan)
        (merge-resilience-context base-context
                                  (resilience-plan-context plan))
        base-context)))

(defun %emit-resilience-operation-event
    (plan handler type context started &optional condition)
  "Emit the terminal composition event for PLAN."
  (let ((event-arguments
          (list :operation (resilience-plan-operation plan)
                :stage :composition
                :context context
                :duration (- (%now
                              :clock (resilience-plan-clock plan)
                              :monotonic-units-per-second
                              (resilience-plan-monotonic-units-per-second plan))
                             started)
                :clock (resilience-plan-clock plan)
                :monotonic-units-per-second
                (resilience-plan-monotonic-units-per-second plan))))
    (when condition
      (setf event-arguments
            (append event-arguments (list :condition condition))))
    (apply #'%emit-resilience-event handler type event-arguments)))

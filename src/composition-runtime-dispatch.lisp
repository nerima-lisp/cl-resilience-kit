(in-package #:resilience-kit)

(defun %run-resilience-plan (plan)
  "Execute PLAN within its dynamic context and lifecycle boundary."
  (let* ((active-context (%plan-active-context plan))
         (active-handler
           (%combined-resilience-handler
            (resilience-plan-event-handler plan)
            (resilience-plan-metrics plan)
            (resilience-plan-observer plan)))
         (active-token
           (%active-cancellation-token
            (resilience-plan-cancellation-token plan)))
         (entered-p nil)
         (started (%now
                   :clock (resilience-plan-clock plan)
                   :monotonic-units-per-second
                   (resilience-plan-monotonic-units-per-second plan))))
    (when (resilience-plan-lifecycle plan)
      (enter-resilience-lifecycle
       (resilience-plan-lifecycle plan)
       :operation (resilience-plan-operation plan))
      (setf entered-p t))
    (unwind-protect
        (let ((*resilience-context* active-context)
              (*resilience-event-handler* active-handler))
          (handler-case
              (multiple-value-prog1
                  (%run-resilience-execution
                   plan active-token active-handler)
                (%emit-resilience-operation-event
                 plan active-handler :operation-complete
                 active-context started))
            (error (condition)
              (%emit-resilience-operation-event
               plan active-handler :operation-failed
               active-context started condition)
              (error condition))))
      (when entered-p
        (leave-resilience-lifecycle (resilience-plan-lifecycle plan))))))

(defun %run-resilience-plan/k (plan on-success on-error)
  "Execute PLAN and dispatch its terminal result to continuations.

The runtime owns operation error handling.  Continuations are invoked only
after that handler has exited, so a continuation error cannot be mistaken for
an operation failure and routed back to ON-ERROR."
  (let ((values nil)
        (condition nil)
        (failed-p nil))
    (handler-case
        (setf values (multiple-value-list (%run-resilience-plan plan)))
      (error (caught-condition)
        (setf condition caught-condition
              failed-p t)))
    (if failed-p
        (funcall on-error condition)
        (apply on-success values))))

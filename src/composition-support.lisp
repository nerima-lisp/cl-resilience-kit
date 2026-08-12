(in-package #:resilience-kit)

(defun %combined-resilience-handler (event-handler metrics observer)
  "Combine optional event, metrics, and observation handlers."
  (let ((handlers
          (remove nil
                  (list event-handler
                        (and (null event-handler)
                             *resilience-event-handler*)
                        (and metrics (resilience-metrics-handler metrics))
                        (and observer (resilience-observer-handler observer))))))
    (cond ((null handlers) nil)
          ((null (rest handlers)) (first handlers))
          (t (lambda (event)
               (dolist (handler handlers)
                 (funcall handler event)))))))

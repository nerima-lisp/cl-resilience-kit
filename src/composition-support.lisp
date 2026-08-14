(in-package #:resilience-kit)

(declaim (inline %combined-resilience-observer-handler))

(defun %combined-resilience-observer-handler (event-handler observer)
  "Combine optional primary and observer handlers."
  (let* ((primary-handler (or event-handler *resilience-event-handler*))
         (observer-handler
           (and observer
                (progn
                  (check-type observer resilience-observer)
                  (resilience-observer-handler observer)))))
    (cond
      ((null primary-handler)
       observer-handler)
      ((null observer-handler)
       primary-handler)
      (t
       (lambda (event)
         (%call-resilience-observer-handler primary-handler event)
         (%call-resilience-observer-handler observer-handler event))))))

(defun %combined-resilience-handler (event-handler metrics observer)
  "Combine optional event, metrics, and observation handlers."
  (let* ((primary-handler (or event-handler *resilience-event-handler*))
         (metrics-instance
           (and metrics
                (progn
                  (check-type metrics resilience-metrics)
                  metrics)))
         (observer-handler
           (%combined-resilience-observer-handler nil observer))
         (variant
           (+ (if primary-handler 1 0)
              (if metrics-instance 2 0)
              (if observer-handler 4 0))))
    (case variant
      (0 nil)
      (1 primary-handler)
      (2 (%make-resilience-metrics-handler metrics-instance))
      (3 (lambda (event)
           (funcall primary-handler event)
           (%record-resilience-event metrics-instance event)))
      (4 observer-handler)
      (5 (lambda (event)
           (%call-resilience-observer-handler primary-handler event)
           (%call-resilience-observer-handler observer-handler event)))
      (6 (lambda (event)
           (%record-resilience-event metrics-instance event)
           (funcall observer-handler event)))
      (7 (lambda (event)
           (funcall primary-handler event)
           (%record-resilience-event metrics-instance event)
           (funcall observer-handler event))))))

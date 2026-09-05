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

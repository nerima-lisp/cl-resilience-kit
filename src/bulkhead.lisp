(in-package #:cl-resilience-kit)

(defmacro with-bulkhead
    ((bulkhead &key operation cancellation-token event-handler timeout)
     &body body)
  "Evaluate BODY only after BULKHEAD admits the operation."
  `(bulkhead-call ,bulkhead (lambda () ,@body)
                  :operation ,operation
                  :cancellation-token ,cancellation-token
                  :event-handler ,event-handler
                  :timeout ,timeout))

(defmacro with-queued-bulkhead
    ((bulkhead &key operation cancellation-token event-handler timeout)
     &body body)
  "Evaluate BODY after a queued bulkhead admits the operation."
  `(queued-bulkhead-call ,bulkhead (lambda () ,@body)
                         :operation ,operation
                         :cancellation-token ,cancellation-token
                         :event-handler ,event-handler
                         :timeout ,timeout))

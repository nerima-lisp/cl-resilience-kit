(in-package #:resilience-kit)

(defmacro with-resilience-executor ((executor &rest options) &body body)
  "Evaluate BODY through RESILIENCE-EXECUTOR-CALL.

EXECUTOR is evaluated once.  OPTIONS are the keyword arguments accepted by
RESILIENCE-EXECUTOR-CALL."
  `(resilience-executor-call ,executor
                             (lambda () ,@body)
                             ,@options))

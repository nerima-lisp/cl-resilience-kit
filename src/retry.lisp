(in-package #:resilience-kit)

(defmacro with-retry ((policy &rest options) &body body)
  "Evaluate BODY through CALL-WITH-RETRY.

POLICY and OPTIONS are evaluated by CALL-WITH-RETRY in the surrounding
lexical environment.  The macro is the block-oriented counterpart of the
first-class CALL-WITH-RETRY function."
  `(call-with-retry ,policy
                    (lambda () ,@body)
                    ,@options))

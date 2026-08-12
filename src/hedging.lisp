(in-package #:resilience-kit)

(defmacro with-hedging ((&rest options) &body body)
  "Evaluate BODY through CALL-WITH-HEDGING.

OPTIONS are the keyword arguments accepted by CALL-WITH-HEDGING.  The
macro keeps the operation in a continuation-shaped thunk while making the
common block form explicit at the call site."
  `(call-with-hedging (lambda () ,@body)
                      ,@options))

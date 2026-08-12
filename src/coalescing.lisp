(in-package #:resilience-kit)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +request-coalescing-option-keys+
    '(:key
      :idempotency-fingerprint
      :executor
      :hard-timeout
      :timeout
      :operation
      :clock
      :monotonic-units-per-second))

  (defun %validate-request-coalescing-options (options)
    (%validate-static-plist-options
     options
     +request-coalescing-option-keys+
     'with-request-coalescing)))

(defmacro with-request-coalescing ((coalescer &rest options) &body body)
  "Evaluate BODY through CALL-WITH-REQUEST-COALESCING.

COALESCER is evaluated once.  Literal OPTIONS are validated at macro-expansion
time and are the keyword arguments accepted by CALL-WITH-REQUEST-COALESCING."
  (%validate-request-coalescing-options options)
  `(call-with-request-coalescing
    ,coalescer (lambda () ,@body) ,@options))

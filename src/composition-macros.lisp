(in-package #:resilience-kit)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun %validate-resilience-options (options macro-name)
    (%validate-static-plist-options
     options
     +resilience-option-keys+
     macro-name)))

(defmacro with-resilience ((&rest options) &body body)
  "Evaluate BODY using CALL-WITH-RESILIENCE's keyword options.

The macro validates the static option list and leaves orchestration to the
runtime function, keeping the expansion small and inspectable."
  (%validate-resilience-options options 'with-resilience)
  `(call-with-resilience (lambda () ,@body) ,@options))

(defmacro with-resilience/k ((on-success on-error &rest options) &body body)
  "Evaluate BODY and dispatch its values to ON-SUCCESS or ON-ERROR.

ON-SUCCESS and ON-ERROR are callback forms.  OPTIONS are validated at macro
expansion time and passed through to CALL-WITH-RESILIENCE/K."
  (%validate-resilience-options options 'with-resilience/k)
  `(call-with-resilience/k (lambda () ,@body)
                           ,on-success
                           ,on-error
                           ,@options))

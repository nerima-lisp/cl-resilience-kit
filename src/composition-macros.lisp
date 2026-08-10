(in-package #:cl-resilience-kit)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +resilience-option-keys+
    '(:retry-policy
      :circuit-breaker
      :distributed-circuit-breaker
      :bulkhead
      :bulkhead-timeout
      :rate-limiter
      :rate-limit-tokens
      :rate-limit-wait-p
      :rate-limit-max-wait
      :rate-limit-signal-on-reject-p
      :overall-timeout
      :overall-deadline
      :per-attempt-timeout
      :clock
      :monotonic-units-per-second
      :sleeper
      :operation
      :retry-budget
      :cancellation-token
      :event-handler
      :fallback
      :context
      :metrics
      :observer
      :lifecycle
      :executor
      :executor-timeout
      :hard-timeout
      :hedge-after
      :max-hedge-attempts
      :hedge-safe-p
      :request-coalescer
      :idempotency-key
      :idempotency-fingerprint))

  (defun %validate-resilience-options (options macro-name)
    (let ((length (list-length options)))
      (unless (or (null options) length)
        (error "~S options must be a proper property list: ~S"
               macro-name options))
      (let ((length (or length 0)))
        (unless (evenp length)
          (error "~S options must contain keyword/value pairs: ~S"
                 macro-name options)))
      (loop for key in options by #'cddr
            do (unless (keywordp key)
                 (error "~S option keys must be keywords: ~S"
                        macro-name key))
               (unless (member key +resilience-option-keys+ :test #'eq)
                 (error "Unknown ~S option ~S" macro-name key))
               (when (member key (cddr (member key options :test #'eq))
                             :test #'eq)
                 (error "Duplicate ~S option ~S" macro-name key)))
      t)))

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

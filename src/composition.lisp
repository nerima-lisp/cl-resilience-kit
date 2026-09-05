(in-package #:resilience-kit)

(declaim (inline %call-with-resilience-dispatch-mode
                 %call-with-resilience-runtime-direct-options-p
                 %call-with-resilience-metrics-direct-options-p
                 %call-with-resilience-core-direct))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +resilience-option-specs+
    '((:retry-policy retry-policy)
      (:circuit-breaker circuit-breaker)
      (:distributed-circuit-breaker distributed-circuit-breaker)
      (:bulkhead bulkhead)
      (:bulkhead-timeout bulkhead-timeout)
      (:rate-limiter rate-limiter)
      (:rate-limit-tokens rate-limit-tokens 1d0)
      (:rate-limit-wait-p rate-limit-wait-p)
      (:rate-limit-max-wait rate-limit-max-wait)
      (:rate-limit-signal-on-reject-p rate-limit-signal-on-reject-p t)
      (:overall-timeout overall-timeout)
      (:overall-deadline overall-deadline)
      (:per-attempt-timeout per-attempt-timeout)
      (:clock clock)
      (:monotonic-units-per-second monotonic-units-per-second)
      (:sleeper sleeper)
      (:operation operation)
      (:retry-budget retry-budget)
      (:cancellation-token cancellation-token)
      (:event-handler event-handler)
      (:fallback fallback)
      (:context context)
      (:metrics metrics)
      (:observer observer)
      (:lifecycle lifecycle)
      (:executor executor)
      (:executor-timeout executor-timeout)
      (:hard-timeout hard-timeout)
      (:hedge-after hedge-after)
      (:max-hedge-attempts max-hedge-attempts)
      (:hedge-safe-p hedge-safe-p)
      (:request-coalescer request-coalescer)
      (:idempotency-key idempotency-key)
      (:idempotency-fingerprint idempotency-fingerprint)))

  (defparameter +resilience-option-keys+
    (mapcar #'first +resilience-option-specs+))

  (defparameter +call-with-resilience-core-direct-option-keys+
    '(:retry-policy
      :circuit-breaker
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
      :fallback))

  (defparameter +call-with-resilience-metrics-direct-option-keys+
    '(:retry-policy
      :circuit-breaker
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
      :fallback
      :metrics))

  (defparameter +call-with-resilience-runtime-direct-option-keys+
    '(:retry-policy
      :circuit-breaker
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
      :idempotency-key))

  (defun %call-with-resilience-core-direct-option-key-p (key)
    (member key
            +call-with-resilience-core-direct-option-keys+
            :test #'eq))

  (defun %call-with-resilience-core-direct-options-p (options)
    (loop for key in options by #'cddr
          always (%call-with-resilience-core-direct-option-key-p key)))

  (defun %call-with-resilience-runtime-direct-options-p (options)
    (loop with saw-runtime-direct-trigger-p = nil
          for cursor on options by #'cddr
          for key = (car cursor)
          for value = (cadr cursor)
          always (member key
                         +call-with-resilience-runtime-direct-option-keys+
                         :test #'eq)
          do (when (and value
                        (or (eq key :event-handler)
                            (eq key :context)
                            (eq key :observer)
                            (eq key :lifecycle)
                            (eq key :idempotency-key)))
               (setf saw-runtime-direct-trigger-p t))
          finally (return saw-runtime-direct-trigger-p)))

  (defun %call-with-resilience-metrics-direct-options-p (options)
    (loop with saw-metrics-p = nil
          for cursor on options by #'cddr
          for key = (car cursor)
          for value = (cadr cursor)
          always (member key
                         +call-with-resilience-metrics-direct-option-keys+
                         :test #'eq)
          do (when (and (eq key :metrics) value)
               (setf saw-metrics-p t))
          finally (return saw-metrics-p))))

  (defun %resilience-option-form (options key &optional default)
    (if (member key options :test #'eq)
        (getf options key)
        default))

  (defun %resilience-rate-limit-tokens-form (options)
    (or (%resilience-option-form options :rate-limit-tokens)
        1d0))

  (defun %resilience-rate-limit-signal-form (options)
    (if (member :rate-limit-signal-on-reject-p options :test #'eq)
        (getf options :rate-limit-signal-on-reject-p)
        t))

  (defun %resilience-direct-arguments (options mode)
    (append
     (list (%resilience-option-form options :retry-policy)
           (%resilience-option-form options :circuit-breaker)
           (%resilience-option-form options :bulkhead)
           (%resilience-option-form options :bulkhead-timeout)
           (%resilience-option-form options :rate-limiter)
           (%resilience-rate-limit-tokens-form options)
           (%resilience-option-form options :rate-limit-wait-p)
           (%resilience-option-form options :rate-limit-max-wait)
           (%resilience-rate-limit-signal-form options)
           (%resilience-option-form options :overall-timeout)
           (%resilience-option-form options :overall-deadline)
           (%resilience-option-form options :per-attempt-timeout)
           (%resilience-option-form options :clock)
           (%resilience-option-form options :monotonic-units-per-second)
           (%resilience-option-form options :sleeper)
           (%resilience-option-form options :operation)
           (%resilience-option-form options :retry-budget)
           (%resilience-option-form options :cancellation-token))
     (ecase mode
       (:core
        (list (%resilience-option-form options :event-handler)
              (%resilience-option-form options :fallback)))
       (:metrics
        (list (%resilience-option-form options :fallback)
              (%resilience-option-form options :metrics)))
       (:runtime
        (list (%resilience-option-form options :event-handler)
              (%resilience-option-form options :fallback)
              (%resilience-option-form options :context)
              (%resilience-option-form options :metrics)
              (%resilience-option-form options :observer)
              (%resilience-option-form options :lifecycle)
              (%resilience-option-form options :idempotency-key))))))

  (defun %resilience-direct-call-form (name thunk options mode)
    (cons name
          (cons thunk
                (%resilience-direct-arguments options mode))))

  (defun %resilience-k-direct-form (on-success on-error direct-form)
    (list 'let
          (list (list '%on-success on-success)
                (list '%on-error on-error))
          (list 'check-type '%on-success 'function)
          (list 'check-type '%on-error 'function)
          (list 'let
                (list (list '%on-success-values nil)
                      (list '%on-success-condition nil)
                      (list '%on-success-failed-p nil))
                (list 'handler-case
                      (list 'setf '%on-success-values
                            (list '%pack-resilience-values direct-form))
                      (list 'error
                            (list 'condition)
                            (list 'setf '%on-success-condition 'condition
                                  '%on-success-failed-p t)))
                (list 'if '%on-success-failed-p
                      (list 'funcall '%on-error '%on-success-condition)
                      (list 'multiple-value-call '%on-success
                            (list '%unpack-resilience-values '%on-success-values))))))
  (defun %resilience-direct-call-form-with-inherited-guard (name thunk options mode)
    "Guard NAME's direct call so a non-nil *RESILIENCE-EVENT-HANDLER* inherited
at runtime falls back to the general dispatch path instead of being silently
dropped by the macroexpansion-time fast path."
    (list 'if '*resilience-event-handler*
          (list '%call-with-resilience-from-options thunk (cons 'list options))
          (%resilience-direct-call-form name thunk options mode)))
  (defun %resilience-k-direct-form-with-inherited-guard
      (on-success on-error name thunk options mode)
    "Continuation-passing counterpart of
%RESILIENCE-DIRECT-CALL-FORM-WITH-INHERITED-GUARD."
    (list 'if '*resilience-event-handler*
          (list '%call-with-resilience/k-from-options
                thunk on-success on-error (cons 'list options))
          (%resilience-k-direct-form
           on-success
           on-error
           (%resilience-direct-call-form name thunk options mode))))

(defun %call-with-resilience-dispatch-mode
    (distributed-circuit-breaker operation event-handler context metrics observer
     lifecycle executor executor-timeout hard-timeout hedge-after
     max-hedge-attempts hedge-safe-p request-coalescer idempotency-key
     idempotency-fingerprint)
  (declare (ignore operation))
  (if (or distributed-circuit-breaker
          executor
          executor-timeout
          hard-timeout
          hedge-after
          max-hedge-attempts
          hedge-safe-p
          request-coalescer
          idempotency-fingerprint)
      :plan
      (if (or event-handler
              context
              observer
              lifecycle
              idempotency-key
              *resilience-event-handler*)
          :runtime-direct
          (if metrics
              :metrics-direct
              :core-direct))))

(defun %parse-resilience-options (options)
  (%validate-static-plist-options options
                                  +resilience-option-keys+
                                  'call-with-resilience)
  (let ((retry-policy nil)
        (circuit-breaker nil)
        (distributed-circuit-breaker nil)
        (bulkhead nil)
        (bulkhead-timeout nil)
        (rate-limiter nil)
        (rate-limit-tokens 1d0)
        (rate-limit-wait-p nil)
        (rate-limit-max-wait nil)
        (rate-limit-signal-on-reject-p t)
        (overall-timeout nil)
        (overall-deadline nil)
        (per-attempt-timeout nil)
        (clock nil)
        (monotonic-units-per-second nil)
        (sleeper nil)
        (operation nil)
        (retry-budget nil)
        (cancellation-token nil)
        (event-handler nil)
        (fallback nil)
        (context nil)
        (metrics nil)
        (observer nil)
        (lifecycle nil)
        (executor nil)
        (executor-timeout nil)
        (hard-timeout nil)
        (hedge-after nil)
        (max-hedge-attempts nil)
        (hedge-safe-p nil)
        (request-coalescer nil)
        (idempotency-key nil)
        (idempotency-fingerprint nil))
    (loop
      for cursor on options by #'cddr
      for key = (car cursor)
      do
         (let ((value (cadr cursor)))
           (case key
             (:retry-policy (setf retry-policy value))
             (:circuit-breaker (setf circuit-breaker value))
             (:distributed-circuit-breaker
              (setf distributed-circuit-breaker value))
             (:bulkhead (setf bulkhead value))
             (:bulkhead-timeout (setf bulkhead-timeout value))
             (:rate-limiter (setf rate-limiter value))
             (:rate-limit-tokens (setf rate-limit-tokens value))
             (:rate-limit-wait-p (setf rate-limit-wait-p value))
             (:rate-limit-max-wait (setf rate-limit-max-wait value))
             (:rate-limit-signal-on-reject-p
              (setf rate-limit-signal-on-reject-p value))
             (:overall-timeout (setf overall-timeout value))
             (:overall-deadline (setf overall-deadline value))
             (:per-attempt-timeout (setf per-attempt-timeout value))
             (:clock (setf clock value))
             (:monotonic-units-per-second
              (setf monotonic-units-per-second value))
             (:sleeper (setf sleeper value))
             (:operation (setf operation value))
             (:retry-budget (setf retry-budget value))
             (:cancellation-token (setf cancellation-token value))
             (:event-handler (setf event-handler value))
             (:fallback (setf fallback value))
             (:context (setf context value))
             (:metrics (setf metrics value))
             (:observer (setf observer value))
             (:lifecycle (setf lifecycle value))
             (:executor (setf executor value))
             (:executor-timeout (setf executor-timeout value))
             (:hard-timeout (setf hard-timeout value))
             (:hedge-after (setf hedge-after value))
             (:max-hedge-attempts (setf max-hedge-attempts value))
             (:hedge-safe-p (setf hedge-safe-p value))
             (:request-coalescer (setf request-coalescer value))
             (:idempotency-key (setf idempotency-key value))
             (:idempotency-fingerprint
              (setf idempotency-fingerprint value)))))
    (values retry-policy
            circuit-breaker
            distributed-circuit-breaker
            bulkhead
            bulkhead-timeout
            rate-limiter
            rate-limit-tokens
            rate-limit-wait-p
            rate-limit-max-wait
            rate-limit-signal-on-reject-p
            overall-timeout
            overall-deadline
            per-attempt-timeout
            clock
            monotonic-units-per-second
            sleeper
            operation
            retry-budget
            cancellation-token
            event-handler
            fallback
            context
            metrics
            observer
            lifecycle
            executor
            executor-timeout
            hard-timeout
            hedge-after
            max-hedge-attempts
            hedge-safe-p
            request-coalescer
            idempotency-key
            idempotency-fingerprint)))

(defun %make-resilience-plan* (thunk
                               retry-policy circuit-breaker distributed-circuit-breaker
                               bulkhead bulkhead-timeout rate-limiter
                               rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
                               rate-limit-signal-on-reject-p
                               overall-timeout overall-deadline per-attempt-timeout
                               clock monotonic-units-per-second sleeper operation
                               retry-budget cancellation-token event-handler fallback
                               context metrics observer lifecycle executor executor-timeout
                               hard-timeout hedge-after max-hedge-attempts hedge-safe-p
                               request-coalescer idempotency-key
                               idempotency-fingerprint)
  (check-type thunk function)
  (when context
    (check-type context resilience-context))
  (when metrics
    (check-type metrics resilience-metrics))
  (when observer
    (unless (resilience-observer-p observer)
      (error "Not a resilience observer: ~S" observer)))
  (when lifecycle
    (check-type lifecycle resilience-lifecycle))
  (when executor
    (check-type executor resilience-executor))
  (%make-resilience-plan
   :thunk thunk
   :retry-policy retry-policy
   :circuit-breaker circuit-breaker
   :distributed-circuit-breaker distributed-circuit-breaker
   :bulkhead bulkhead
   :bulkhead-timeout bulkhead-timeout
   :rate-limiter rate-limiter
   :rate-limit-tokens rate-limit-tokens
   :rate-limit-wait-p rate-limit-wait-p
   :rate-limit-max-wait rate-limit-max-wait
   :rate-limit-signal-on-reject-p rate-limit-signal-on-reject-p
   :overall-timeout overall-timeout
   :overall-deadline overall-deadline
   :per-attempt-timeout per-attempt-timeout
   :clock clock
   :monotonic-units-per-second monotonic-units-per-second
   :sleeper sleeper
   :operation operation
   :retry-budget retry-budget
   :cancellation-token cancellation-token
   :event-handler event-handler
   :fallback fallback
   :context context
   :metrics metrics
   :observer observer
   :lifecycle lifecycle
   :executor executor
   :executor-timeout executor-timeout
   :hard-timeout hard-timeout
   :hedge-after hedge-after
   :max-hedge-attempts max-hedge-attempts
   :hedge-safe-p hedge-safe-p
   :request-coalescer request-coalescer
   :idempotency-key idempotency-key
   :idempotency-fingerprint idempotency-fingerprint))

(defun %call-with-resilience/k-from-options
    (thunk on-success on-error options)
  (multiple-value-call
      (lambda (retry-policy circuit-breaker distributed-circuit-breaker
                            bulkhead bulkhead-timeout rate-limiter rate-limit-tokens
                            rate-limit-wait-p rate-limit-max-wait
                            rate-limit-signal-on-reject-p overall-timeout overall-deadline
                            per-attempt-timeout clock monotonic-units-per-second sleeper
                            operation retry-budget cancellation-token event-handler fallback
                            context metrics observer lifecycle executor executor-timeout
                            hard-timeout hedge-after max-hedge-attempts hedge-safe-p
                            request-coalescer idempotency-key idempotency-fingerprint)
        (case (%call-with-resilience-dispatch-mode
               distributed-circuit-breaker operation event-handler context
               metrics observer lifecycle executor executor-timeout hard-timeout
               hedge-after max-hedge-attempts hedge-safe-p request-coalescer
               idempotency-key idempotency-fingerprint)
          (:metrics-direct
           (check-type metrics resilience-metrics)
           (let ((%values nil)
                 (%condition nil)
                 (%failed-p nil))
             (handler-case
                 (setf %values
                       (%pack-resilience-values
                        (%run-resilience-metrics-direct
                         thunk retry-policy circuit-breaker bulkhead bulkhead-timeout
                         rate-limiter rate-limit-tokens rate-limit-wait-p
                         rate-limit-max-wait rate-limit-signal-on-reject-p
                         overall-timeout overall-deadline per-attempt-timeout clock
                         monotonic-units-per-second sleeper operation retry-budget
                         cancellation-token fallback metrics)))
               (error (condition)
                      (setf %condition condition
                            %failed-p t)))
             (if %failed-p
                 (funcall on-error %condition)
                 (multiple-value-call on-success (%unpack-resilience-values %values)))))
          (:core-direct
           (let ((%values nil)
                 (%condition nil)
                 (%failed-p nil))
             (handler-case
                 (setf %values
                       (%pack-resilience-values
                        (%call-with-resilience-core-direct
                         thunk retry-policy circuit-breaker bulkhead bulkhead-timeout
                         rate-limiter rate-limit-tokens rate-limit-wait-p
                         rate-limit-max-wait rate-limit-signal-on-reject-p
                         overall-timeout overall-deadline per-attempt-timeout clock
                         monotonic-units-per-second sleeper operation retry-budget
                         cancellation-token event-handler fallback)))
               (error (condition)
                      (setf %condition condition
                            %failed-p t)))
             (if %failed-p
                 (funcall on-error %condition)
                 (multiple-value-call on-success (%unpack-resilience-values %values)))))
          (:runtime-direct
           (%run-resilience-runtime-direct/k
            on-success on-error thunk retry-policy circuit-breaker bulkhead
            bulkhead-timeout rate-limiter rate-limit-tokens rate-limit-wait-p
            rate-limit-max-wait rate-limit-signal-on-reject-p overall-timeout
            overall-deadline per-attempt-timeout clock
            monotonic-units-per-second sleeper operation retry-budget
            cancellation-token event-handler fallback context metrics observer
            lifecycle idempotency-key))
          (otherwise
           (%run-resilience-plan/k
            (%make-resilience-plan*
             thunk retry-policy circuit-breaker distributed-circuit-breaker
             bulkhead bulkhead-timeout rate-limiter rate-limit-tokens
             rate-limit-wait-p rate-limit-max-wait
             rate-limit-signal-on-reject-p overall-timeout overall-deadline
             per-attempt-timeout clock monotonic-units-per-second sleeper
             operation retry-budget cancellation-token event-handler fallback
             context metrics observer lifecycle executor executor-timeout
             hard-timeout hedge-after max-hedge-attempts hedge-safe-p
             request-coalescer idempotency-key idempotency-fingerprint)
            on-success on-error))))
    (%parse-resilience-options options)))

(defun %call-with-resilience-from-options (thunk options)
  (multiple-value-call
      (lambda (retry-policy circuit-breaker distributed-circuit-breaker
               bulkhead bulkhead-timeout rate-limiter rate-limit-tokens
               rate-limit-wait-p rate-limit-max-wait
               rate-limit-signal-on-reject-p overall-timeout overall-deadline
               per-attempt-timeout clock monotonic-units-per-second sleeper
               operation retry-budget cancellation-token event-handler fallback
               context metrics observer lifecycle executor executor-timeout
               hard-timeout hedge-after max-hedge-attempts hedge-safe-p
               request-coalescer idempotency-key idempotency-fingerprint)
        (case (%call-with-resilience-dispatch-mode
               distributed-circuit-breaker operation event-handler context
               metrics observer lifecycle executor executor-timeout hard-timeout
               hedge-after max-hedge-attempts hedge-safe-p request-coalescer
               idempotency-key idempotency-fingerprint)
          (:metrics-direct
           (check-type metrics resilience-metrics)
           (%run-resilience-metrics-direct
            thunk retry-policy circuit-breaker bulkhead bulkhead-timeout
            rate-limiter rate-limit-tokens rate-limit-wait-p
            rate-limit-max-wait rate-limit-signal-on-reject-p overall-timeout
            overall-deadline per-attempt-timeout clock
            monotonic-units-per-second sleeper operation retry-budget
            cancellation-token fallback metrics))
          (:core-direct
           (%call-with-resilience-core-direct
            thunk retry-policy circuit-breaker bulkhead bulkhead-timeout
            rate-limiter rate-limit-tokens rate-limit-wait-p
            rate-limit-max-wait rate-limit-signal-on-reject-p overall-timeout
            overall-deadline per-attempt-timeout clock
            monotonic-units-per-second sleeper operation retry-budget
            cancellation-token event-handler fallback))
          (:runtime-direct
           (%run-resilience-runtime-direct
            thunk retry-policy circuit-breaker bulkhead bulkhead-timeout
            rate-limiter rate-limit-tokens rate-limit-wait-p
            rate-limit-max-wait rate-limit-signal-on-reject-p overall-timeout
            overall-deadline per-attempt-timeout clock
            monotonic-units-per-second sleeper operation retry-budget
            cancellation-token event-handler fallback context metrics observer
            lifecycle idempotency-key))
          (otherwise
           (%run-resilience-plan
            (%make-resilience-plan*
             thunk retry-policy circuit-breaker distributed-circuit-breaker
             bulkhead bulkhead-timeout rate-limiter rate-limit-tokens
             rate-limit-wait-p rate-limit-max-wait
             rate-limit-signal-on-reject-p overall-timeout overall-deadline
             per-attempt-timeout clock monotonic-units-per-second sleeper
             operation retry-budget cancellation-token event-handler fallback
             context metrics observer lifecycle executor executor-timeout
             hard-timeout hedge-after max-hedge-attempts hedge-safe-p
             request-coalescer idempotency-key idempotency-fingerprint)))))
    (%parse-resilience-options options)))

(defun call-with-resilience/k (thunk on-success on-error &rest options)
  "Call THUNK with resilience OPTIONS and dispatch its result.

All values from a successful call are passed to ON-SUCCESS.  A condition of
type ERROR signaled by the resilient call is passed as the sole argument to
ON-ERROR.  Errors raised by either continuation are intentionally allowed to
escape, so continuation failures are not mistaken for operation failures."
  (check-type thunk function)
  (check-type on-success function)
  (check-type on-error function)
  (%call-with-resilience/k-from-options
   thunk on-success on-error options))

(defun call-with-resilience (thunk &rest options)
  "Compose local and distributed resilience controls around THUNK.

The v2 composition boundary combines retry, bulkhead, rate-limit, and
circuit-breaker controls with optional execution-boundary controls.  EXECUTOR,
HARD-TIMEOUT, HEDGE-AFTER, and REQUEST-COALESCER require the explicit
idempotency and cancellation choices documented by their respective APIs.
OPTIONS is a keyword property list; its values are validated before dispatch."
  (check-type thunk function)
  (%call-with-resilience-from-options thunk options))

(define-compiler-macro call-with-resilience (&whole form thunk &rest options)
  (cond
    ((null options)
     (%resilience-direct-call-form-with-inherited-guard
      '%call-with-resilience-core-direct
      thunk
      nil
      :core))
    ((not (evenp (length options)))
     form)
    ((loop for key in options by #'cddr
           always (keywordp key))
     (cond
       ((%call-with-resilience-core-direct-options-p options)
        (if (%call-with-resilience-runtime-direct-options-p options)
            (%resilience-direct-call-form
             '%run-resilience-runtime-direct
             thunk
             options
             :runtime)
            (%resilience-direct-call-form-with-inherited-guard
             '%call-with-resilience-core-direct
             thunk
             options
             :core)))
       ((%call-with-resilience-metrics-direct-options-p options)
        (if (%call-with-resilience-runtime-direct-options-p options)
            (%resilience-direct-call-form
             '%run-resilience-metrics-direct
             thunk
             options
             :metrics)
            (%resilience-direct-call-form-with-inherited-guard
             '%run-resilience-metrics-direct
             thunk
             options
             :metrics)))
       ((%call-with-resilience-runtime-direct-options-p options)
        (%resilience-direct-call-form
         '%run-resilience-runtime-direct
         thunk
         options
         :runtime))
       (t
        form)))
    (t
     form)))

(define-compiler-macro call-with-resilience/k
    (&whole form thunk on-success on-error &rest options)
  (cond
    ((null options)
     (%resilience-k-direct-form-with-inherited-guard
      on-success
      on-error
      '%call-with-resilience-core-direct
      thunk
      nil
      :core))
    ((not (evenp (length options)))
     form)
    ((loop for key in options by #'cddr
           always (keywordp key))
     (cond
       ((%call-with-resilience-core-direct-options-p options)
        (if (%call-with-resilience-runtime-direct-options-p options)
            (%resilience-k-direct-form
             on-success
             on-error
             (%resilience-direct-call-form
              '%run-resilience-runtime-direct
              thunk
              options
              :runtime))
            (%resilience-k-direct-form-with-inherited-guard
             on-success
             on-error
             '%call-with-resilience-core-direct
             thunk
             options
             :core)))
       ((%call-with-resilience-metrics-direct-options-p options)
        (if (%call-with-resilience-runtime-direct-options-p options)
            (%resilience-k-direct-form
             on-success
             on-error
             (%resilience-direct-call-form
              '%run-resilience-metrics-direct
              thunk
              options
              :metrics))
            (%resilience-k-direct-form-with-inherited-guard
             on-success
             on-error
             '%run-resilience-metrics-direct
             thunk
             options
             :metrics)))
       ((%call-with-resilience-runtime-direct-options-p options)
        (%resilience-k-direct-form
         on-success
         on-error
         (%resilience-direct-call-form
          '%run-resilience-runtime-direct
          thunk
          options
          :runtime)))
       (t
        form)))
    (t
     form)))

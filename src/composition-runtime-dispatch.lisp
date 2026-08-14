(in-package #:resilience-kit)

(defun %run-resilience-plan (plan)
  "Execute PLAN within its dynamic context and lifecycle boundary."
  (let* ((active-context (%plan-active-context plan))
         (event-handler (resilience-plan-event-handler plan))
         (metrics (resilience-plan-metrics plan))
         (observer (resilience-plan-observer plan))
         (metrics-only-p
           (and metrics
                (null event-handler)
                (null observer)
                (null *resilience-event-handler*)))
         (active-handler
           (and (not metrics-only-p)
                (%combined-resilience-observer-handler
                 event-handler observer)))
         (cancellation-token
           (resilience-plan-cancellation-token plan))
         (active-token
           (%active-cancellation-token
            cancellation-token))
         (clock (resilience-plan-clock plan))
         (units (resilience-plan-monotonic-units-per-second plan))
         (lifecycle (resilience-plan-lifecycle plan))
         (operation (resilience-plan-operation plan))
         (entered-p nil)
         (started (and (or active-handler metrics-only-p)
                       (%monotonic-seconds clock units))))
    (if (and (null active-handler)
             (not metrics-only-p)
             (null metrics)
             (null lifecycle))
        (let ((*resilience-context* active-context)
              (*resilience-event-handler* nil)
              (*resilience-event-metrics* nil))
          (%run-resilience-execution plan active-token nil))
        (progn
          (when lifecycle
            (enter-resilience-lifecycle
             lifecycle
             :operation operation)
            (setf entered-p t))
          (unwind-protect
              (let ((*resilience-context* active-context)
                    (*resilience-event-handler* active-handler)
                    (*resilience-event-metrics* metrics))
                (let ((operation-complete-p nil)
                      (operation-condition nil))
                  (unwind-protect
                      (handler-case
                          (multiple-value-prog1
                              (%run-resilience-execution
                               plan active-token active-handler)
                            (setf operation-complete-p t))
                        (error (condition)
                          (setf operation-condition condition)
                          (values)))
                    (if operation-complete-p
                        (when started
                          (%emit-resilience-operation-event
                           plan active-handler metrics metrics-only-p
                           :operation-complete active-context started))
                        (progn
                          (when started
                            (%emit-resilience-operation-event
                             plan active-handler metrics metrics-only-p
                             :operation-failed active-context started
                             operation-condition))
                          (error operation-condition)))))
            (when entered-p
              (leave-resilience-lifecycle lifecycle))))))))

(defun %run-resilience-plan/k (plan on-success on-error)
  "Execute PLAN and dispatch its terminal result to continuations.

The runtime owns operation error handling.  Continuations are invoked only
after that handler has exited, so a continuation error cannot be mistaken for
an operation failure and routed back to ON-ERROR."
  (let ((values nil)
        (condition nil)
        (failed-p nil))
    (handler-case
        (setf values (%pack-resilience-values (%run-resilience-plan plan)))
      (error (caught-condition)
        (setf condition caught-condition
              failed-p t)))
    (if failed-p
        (funcall on-error condition)
        (multiple-value-call on-success
          (%unpack-resilience-values values)))))

(defun %call-with-resilience-active-context
    (operation context idempotency-key)
  (let ((current-context *resilience-context*))
    (if (or operation idempotency-key)
        (let ((base-context
                (%merge-resilience-context-operation
                 current-context
                 operation
                 idempotency-key)))
          (if context
              (merge-resilience-context base-context context)
              base-context))
        (if context
            (if current-context
                (merge-resilience-context current-context context)
                (progn
                  (check-type context resilience-context)
                  context))
            current-context))))

(defun %call-with-resilience-core-direct
    (thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
     rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
     rate-limit-signal-on-reject-p overall-timeout overall-deadline
     per-attempt-timeout clock monotonic-units-per-second sleeper operation
     retry-budget cancellation-token event-handler fallback)
  (%call-with-resilience-core*
   thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
   rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
   rate-limit-signal-on-reject-p overall-timeout overall-deadline
   per-attempt-timeout clock monotonic-units-per-second sleeper operation
   retry-budget cancellation-token event-handler fallback))

(defun %emit-resilience-direct-operation-event
    (handler metrics metrics-only-p type operation context started
     clock monotonic-units-per-second &optional condition)
  (let* ((finished-at (%monotonic-seconds clock monotonic-units-per-second))
         (duration (- finished-at started)))
    (if metrics-only-p
        (%record-resilience-event* metrics type operation duration)
        (%emit-resilience-event* handler
                                 type
                                 operation
                                 nil
                                 :composition
                                 condition
                                 nil
                                 nil
                                 nil
                                 finished-at
                                 context
                                 nil
                                 duration
                                 clock
                                 monotonic-units-per-second))))

(defun %run-resilience-metrics-direct
    (thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
     rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
     rate-limit-signal-on-reject-p overall-timeout overall-deadline
     per-attempt-timeout clock monotonic-units-per-second sleeper operation
     retry-budget cancellation-token fallback metrics)
  (let* ((active-token (%active-cancellation-token cancellation-token))
         (active-clock (%active-clock clock))
         (active-units
           (%active-monotonic-units-per-second
            monotonic-units-per-second))
         (started (%monotonic-seconds active-clock active-units))
         (operation-complete-p nil)
         (operation-condition nil))
    (let ((*resilience-event-metrics* metrics))
      (unwind-protect
           (handler-case
               (multiple-value-prog1
                    (%call-with-resilience-core-direct
                     thunk
                     retry-policy
                     circuit-breaker
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
                     active-clock
                     active-units
                     sleeper
                     operation
                     retry-budget
                     active-token
                     nil
                     fallback)
                 (setf operation-complete-p t))
             (error (condition)
               (setf operation-condition condition)
               (values)))
        (let ((duration (- (%monotonic-seconds
                            active-clock
                            active-units)
                           started)))
          (if operation-complete-p
              (%record-resilience-event* metrics
                                         :operation-complete
                                         operation
                                         duration)
              (progn
                (%record-resilience-event* metrics
                                           :operation-failed
                                           operation
                                           duration)
                (error operation-condition))))))))

(defun %run-resilience-runtime-direct
    (thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
     rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
     rate-limit-signal-on-reject-p overall-timeout overall-deadline
     per-attempt-timeout clock monotonic-units-per-second sleeper operation
     retry-budget cancellation-token event-handler fallback context metrics
     observer lifecycle idempotency-key)
  (let* ((active-context
           (%call-with-resilience-active-context
            operation context idempotency-key))
         (metrics-only-p
           (and metrics
                (null event-handler)
                (null observer)
                (null *resilience-event-handler*)))
         (active-handler
           (and (not metrics-only-p)
                (%combined-resilience-observer-handler
                 event-handler observer)))
         (active-token
           (%active-cancellation-token cancellation-token))
         (active-clock (%active-clock clock))
         (active-units
           (%active-monotonic-units-per-second
            monotonic-units-per-second))
         (entered-p nil))
    (when lifecycle
      (enter-resilience-lifecycle lifecycle :operation operation)
      (setf entered-p t))
    (unwind-protect
        (let ((*resilience-context* active-context)
              (*resilience-event-handler* active-handler)
              (*resilience-event-metrics* metrics))
          (if (and (null active-handler)
                   (not metrics-only-p)
                   (null metrics))
              (%call-with-resilience-core-direct
               thunk
               retry-policy
               circuit-breaker
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
               active-clock
               active-units
               sleeper
               operation
               retry-budget
               active-token
               nil
               fallback)
              (let ((started (%monotonic-seconds
                              active-clock active-units))
                    (operation-complete-p nil)
                    (operation-condition nil))
                (unwind-protect
                     (handler-case
                         (multiple-value-prog1
                              (%call-with-resilience-core-direct
                               thunk
                               retry-policy
                               circuit-breaker
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
                               active-clock
                               active-units
                               sleeper
                               operation
                               retry-budget
                               active-token
                               active-handler
                               fallback)
                           (setf operation-complete-p t))
                       (error (condition)
                         (setf operation-condition condition)
                         (values)))
                  (if operation-complete-p
                      (%emit-resilience-direct-operation-event
                       active-handler metrics metrics-only-p
                       :operation-complete operation active-context started
                       active-clock active-units)
                      (progn
                        (%emit-resilience-direct-operation-event
                         active-handler metrics metrics-only-p
                         :operation-failed operation active-context started
                         active-clock active-units
                         operation-condition)
                        (error operation-condition)))))))
      (when entered-p
        (leave-resilience-lifecycle lifecycle)))))

(defun %run-resilience-runtime-direct/k
    (on-success on-error
     thunk retry-policy circuit-breaker bulkhead bulkhead-timeout rate-limiter
     rate-limit-tokens rate-limit-wait-p rate-limit-max-wait
     rate-limit-signal-on-reject-p overall-timeout overall-deadline
     per-attempt-timeout clock monotonic-units-per-second sleeper operation
     retry-budget cancellation-token event-handler fallback context metrics
     observer lifecycle idempotency-key)
  (let ((values nil)
        (condition nil)
        (failed-p nil))
    (handler-case
        (setf values
              (%pack-resilience-values
               (%run-resilience-runtime-direct
                thunk
                retry-policy
                circuit-breaker
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
                idempotency-key)))
      (error (caught-condition)
        (setf condition caught-condition
              failed-p t)))
    (if failed-p
        (funcall on-error condition)
        (multiple-value-call on-success
          (%unpack-resilience-values values)))))

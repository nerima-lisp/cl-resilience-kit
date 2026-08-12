(in-package #:resilience-kit)

(defun %retry-result-classification-required-p (policy event-handler)
  (or event-handler
      (and (retry-policy-retry-safe-p policy)
           (retry-policy-result-classifier policy))))

(defun %signal-retry-exhausted
    (policy attempt operation reason clock units event-handler
     &key condition result)
  (let ((exhausted
          (%make-retry-exhausted
           policy attempt condition result reason operation)))
    (%emit-resilience-event
     event-handler :retry-exhausted
     :operation operation :attempt attempt
     :condition condition :result result :reason reason
     :clock clock :monotonic-units-per-second units)
    (error exhausted)))

(defun %continue-retry
    (continue policy attempt previous-delay decision clock units deadline
     sleeper operation retry-budget event-handler
     &key condition result)
  (let ((delay
          (%retry-delay-or-deadline
           policy attempt previous-delay decision
           clock units deadline sleeper operation
           condition result retry-budget event-handler)))
    (funcall continue (1+ attempt) delay)))

(defun %retry-exhausted-p (policy attempt)
  (>= attempt (retry-policy-max-attempts policy)))

(defun %resolve-retry-decision
    (continue policy attempt previous-delay decision
     clock units deadline sleeper operation retry-budget
     event-handler &key condition result)
  (if (%retry-exhausted-p policy attempt)
      (%signal-retry-exhausted
       policy attempt operation
       (retry-decision-reason decision)
       clock units event-handler
       :condition condition :result result)
      (%continue-retry
       continue policy attempt previous-delay decision
       clock units deadline sleeper operation retry-budget
       event-handler :condition condition :result result)))

(defun %handle-retry-condition
    (continue policy attempt previous-delay condition
     clock units deadline sleeper operation retry-budget
     event-handler fallback)
  (%emit-resilience-event
   event-handler :attempt-failure
   :operation operation :attempt attempt
   :condition condition :clock clock
   :monotonic-units-per-second units)
  (let ((decision
          (%retry-decision-for
           policy attempt :condition condition)))
    (if (retry-decision-retry-p decision)
        (%resolve-retry-decision
         continue policy attempt previous-delay decision
         clock units deadline sleeper operation retry-budget
         event-handler :condition condition)
        (%invoke-retry-fallback
         fallback condition event-handler operation attempt
         clock units))))

(defun %handle-retry-result
    (continue policy attempt previous-delay returned
     clock units deadline sleeper operation retry-budget
     event-handler)
  (let* ((result (first returned))
         (decision
           (%retry-decision-for
            policy attempt :result result)))
    (if (retry-decision-retry-p decision)
        (progn
          (%emit-resilience-event
           event-handler :attempt-failure
           :operation operation :attempt attempt
           :result result :reason
           (retry-decision-reason decision)
           :clock clock
           :monotonic-units-per-second units)
          (%resolve-retry-decision
           continue policy attempt previous-delay decision
           clock units deadline sleeper operation retry-budget
           event-handler :result result))
        (progn
          (%emit-resilience-event
           event-handler :attempt-success
           :operation operation :attempt attempt
           :result result :clock clock
           :monotonic-units-per-second units)
          (apply #'values returned)))))

(defun %run-retry-attempt
    (continue policy thunk attempt previous-delay
     clock units deadline per-attempt-timeout sleeper
     operation retry-budget event-handler)
  (%check-active-cancellation-token)
  (%emit-resilience-event
   event-handler :attempt-start
   :operation operation :attempt attempt
   :clock clock
   :monotonic-units-per-second units)
  (if (%retry-result-classification-required-p policy event-handler)
      (%handle-retry-result
       continue policy attempt previous-delay
       (multiple-value-list
        (%execute-attempt
         thunk clock units deadline
         per-attempt-timeout operation attempt))
       clock units deadline sleeper operation retry-budget
       event-handler)
      (%execute-attempt
       thunk clock units deadline
       per-attempt-timeout operation attempt)))

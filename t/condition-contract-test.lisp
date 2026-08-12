(in-package #:resilience-kit/test)

(describe "lifecycle and condition boundary contracts"
  (it "exposes lifecycle admission, timeout, and macro cleanup"
    (let ((lifecycle (resilience-kit:make-resilience-lifecycle)))
      (expect (resilience-kit:resilience-lifecycle-accepting-p lifecycle)
              :to-be-truthy)
      (resilience-kit:enter-resilience-lifecycle lifecycle :operation :wait)
      (expect (resilience-kit:await-resilience-drained
               lifecycle :timeout 0d0)
              :to-be
              nil)
      (expect (resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be
              0)
      (expect (resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be
              0)
      (expect
       (resilience-kit:with-resilience-lifecycle
           (lifecycle :operation :macro)
         :macro-result)
       :to-be
       :macro-result)
      (resilience-kit:begin-resilience-drain lifecycle)
      (expect (resilience-kit:resilience-lifecycle-accepting-p lifecycle)
              :to-be
              nil)))

  (it "keeps structured condition payloads inspectable"
    (let* ((token (gensym "TOKEN"))
           (cause (gensym "CAUSE"))
           (result (gensym "RESULT"))
           (policy (gensym "POLICY"))
           (key (gensym "KEY"))
           (owner (gensym "OWNER"))
           (value (gensym "VALUE")))
      (expect-condition-contracts
       (list
        (list 'resilience-kit:resilience-cancelled
              (list :operation :cancel
                    :token token
                    :reason :shutdown)
              (list #'resilience-kit:resilience-error-operation :cancel)
              (list #'resilience-kit:resilience-cancelled-token token)
              (list #'resilience-kit:resilience-cancelled-reason :shutdown))
        (list 'resilience-kit:retry-exhausted
              (list :operation :retry
                    :attempts 3
                    :last-condition cause
                    :last-result result
                    :reason :budget
                    :policy policy)
              (list #'resilience-kit:resilience-error-operation :retry)
              (list #'resilience-kit:retry-exhausted-attempts 3)
              (list #'resilience-kit:retry-exhausted-last-condition cause)
              (list #'resilience-kit:retry-exhausted-last-result result)
              (list #'resilience-kit:retry-exhausted-reason :budget)
              (list #'resilience-kit:retry-exhausted-policy policy))
        (list 'resilience-kit:retry-classifier-error
              (list :operation :classify :cause cause)
              (list #'resilience-kit:resilience-error-operation :classify)
              (list #'resilience-kit:retry-classifier-error-cause cause))
        (list 'resilience-kit:deadline-exceeded
              (list :operation :deadline
                    :deadline 10
                    :observed-at 11
                    :stage :backoff
                    :attempt 2)
              (list #'resilience-kit:resilience-error-operation :deadline)
              (list #'resilience-kit:deadline-exceeded-deadline 10)
              (list #'resilience-kit:deadline-exceeded-observed-at 11)
              (list #'resilience-kit:deadline-exceeded-stage :backoff)
              (list #'resilience-kit:deadline-exceeded-attempt 2))
        (list 'resilience-kit:attempt-timeout
              (list :operation :attempt
                    :deadline 10
                    :observed-at 11
                    :stage :attempt
                    :attempt 1
                    :timeout 0.5d0)
              (list #'resilience-kit:attempt-timeout-timeout 0.5d0))
        (list 'resilience-kit:circuit-open
              (list :operation :breaker
                    :state :half-open
                    :retry-at 12
                    :generation 3)
              (list #'resilience-kit:circuit-open-state :half-open)
              (list #'resilience-kit:circuit-open-retry-at 12)
              (list #'resilience-kit:circuit-open-generation 3))
        (list 'resilience-kit:bulkhead-rejected
              (list :operation :bulkhead :limit 4 :in-flight 4)
              (list #'resilience-kit:bulkhead-rejected-limit 4)
              (list #'resilience-kit:bulkhead-rejected-in-flight 4))
        (list 'resilience-kit:rate-limit-exceeded
              (list :operation :rate-limit
                    :requested-tokens 3
                    :available-tokens 1
                    :retry-after 2.0d0)
              (list #'resilience-kit:rate-limit-exceeded-requested-tokens 3)
              (list #'resilience-kit:rate-limit-exceeded-available-tokens 1)
              (list #'resilience-kit:rate-limit-exceeded-retry-after 2.0d0))
        (list 'resilience-kit:resilience-store-error
              (list :operation :store :key key :cause cause)
              (list #'resilience-kit:resilience-store-error-key key)
              (list #'resilience-kit:resilience-store-error-cause cause))
        (list 'resilience-kit:resilience-store-conflict
              (list :operation :store
                    :key key
                    :cause cause
                    :expected-version 1
                    :actual-version 2)
              (list #'resilience-kit:resilience-store-conflict-expected-version
                    1)
              (list #'resilience-kit:resilience-store-conflict-actual-version
                    2))
        (list 'resilience-kit:resilience-lease-unavailable
              (list :operation :lease
                    :key key
                    :owner owner
                    :retry-after 3.0d0)
              (list #'resilience-kit:resilience-lease-unavailable-key key)
              (list #'resilience-kit:resilience-lease-unavailable-owner
                    owner)
              (list #'resilience-kit:resilience-lease-unavailable-retry-after
                    3.0d0))
        (list 'resilience-kit:resilience-lease-lost
              (list :operation :lease
                    :key key
                    :owner owner
                    :fencing-token 4)
              (list #'resilience-kit:resilience-lease-lost-key key)
              (list #'resilience-kit:resilience-lease-lost-owner owner)
              (list #'resilience-kit:resilience-lease-lost-fencing-token 4))
        (list 'resilience-kit:stale-fencing-token
              (list :operation :lease
                    :key key
                    :fencing-token 4
                    :current-fencing-token 5)
              (list #'resilience-kit:stale-fencing-token-key key)
              (list #'resilience-kit:stale-fencing-token-fencing-token 4)
              (list #'resilience-kit:stale-fencing-token-current-fencing-token
                    5))
        (list 'resilience-kit:resilience-draining
              (list :operation :lifecycle :state :draining)
              (list #'resilience-kit:resilience-draining-state :draining))
        (list 'resilience-kit:resilience-execution-rejected
              (list :operation :executor
                    :reason :queue-full
                    :queue-size 3)
              (list #'resilience-kit:resilience-execution-rejected-reason
                    :queue-full)
              (list #'resilience-kit:resilience-execution-rejected-queue-size
                    3))
        (list 'resilience-kit:resilience-execution-timeout
              (list :operation :executor :timeout 2.0d0 :backend :thread)
              (list #'resilience-kit:resilience-execution-timeout-timeout
                    2.0d0)
              (list #'resilience-kit:resilience-execution-timeout-backend
                    :thread))
        (list 'resilience-kit:resilience-hard-timeout
              (list :operation :executor
                    :deadline 10
                    :observed-at 11
                    :stage :attempt
                    :attempt 1
                    :timeout 2.0d0
                    :backend :executor)
              (list #'resilience-kit:resilience-hard-timeout-backend
                    :executor))
        (list 'resilience-kit:hedge-unsafe
              (list :operation :hedge :reason :unsafe)
              (list #'resilience-kit:hedge-unsafe-reason :unsafe))
        (list 'resilience-kit:hedge-exhausted
              (list :operation :hedge :causes cause :attempts 2)
              (list #'resilience-kit:hedge-exhausted-causes cause)
              (list #'resilience-kit:hedge-exhausted-attempts 2))
        (list 'resilience-kit:idempotency-key-required
              (list :operation :idempotency))
        (list 'resilience-kit:idempotency-conflict
              (list :operation :idempotency
                    :key key
                    :existing-value value)
              (list #'resilience-kit:idempotency-conflict-key key)
              (list #'resilience-kit:idempotency-conflict-existing-value
                    value))))))
)

(in-package #:cl-resilience-kit/test)

(describe "lifecycle and condition boundary contracts"
  (it "exposes lifecycle admission, timeout, and macro cleanup"
    (let ((lifecycle (cl-resilience-kit:make-resilience-lifecycle)))
      (expect (cl-resilience-kit:resilience-lifecycle-accepting-p lifecycle)
              :to-be-truthy)
      (cl-resilience-kit:enter-resilience-lifecycle lifecycle :operation :wait)
      (expect (cl-resilience-kit:await-resilience-drained
               lifecycle :timeout 0d0)
              :to-be
              nil)
      (expect (cl-resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be
              0)
      (expect
       (cl-resilience-kit:with-resilience-lifecycle
           (lifecycle :operation :macro)
         :macro-result)
       :to-be
       :macro-result)
      (cl-resilience-kit:begin-resilience-drain lifecycle)
      (expect (cl-resilience-kit:resilience-lifecycle-accepting-p lifecycle)
              :to-be
              nil)))

  (it "keeps structured condition payloads inspectable"
    (let* ((token (gensym "TOKEN"))
           (cause (gensym "CAUSE"))
           (result (gensym "RESULT"))
           (policy (gensym "POLICY"))
           (key (gensym "KEY"))
           (owner (gensym "OWNER"))
           (value (gensym "VALUE"))
           (specifications
             (list
              (list 'cl-resilience-kit:resilience-cancelled
                    (list :operation :cancel
                          :token token
                          :reason :shutdown)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :cancel)
                          (list #'cl-resilience-kit:resilience-cancelled-token
                                token)
                          (list #'cl-resilience-kit:resilience-cancelled-reason
                                :shutdown)))
              (list 'cl-resilience-kit:retry-exhausted
                    (list :operation :retry
                          :attempts 3
                          :last-condition cause
                          :last-result result
                          :reason :budget
                          :policy policy)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :retry)
                          (list #'cl-resilience-kit:retry-exhausted-attempts
                                3)
                          (list #'cl-resilience-kit:retry-exhausted-last-condition
                                cause)
                          (list #'cl-resilience-kit:retry-exhausted-last-result
                                result)
                          (list #'cl-resilience-kit:retry-exhausted-reason
                                :budget)
                          (list #'cl-resilience-kit:retry-exhausted-policy
                                policy)))
              (list 'cl-resilience-kit:retry-classifier-error
                    (list :operation :classify :cause cause)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :classify)
                          (list #'cl-resilience-kit:retry-classifier-error-cause
                                cause)))
              (list 'cl-resilience-kit:deadline-exceeded
                    (list :operation :deadline
                          :deadline 10
                          :observed-at 11
                          :stage :backoff
                          :attempt 2)
                    (list (list #'cl-resilience-kit:resilience-error-operation
                                :deadline)
                          (list #'cl-resilience-kit:deadline-exceeded-deadline
                                10)
                          (list #'cl-resilience-kit:deadline-exceeded-observed-at
                                11)
                          (list #'cl-resilience-kit:deadline-exceeded-stage
                                :backoff)
                          (list #'cl-resilience-kit:deadline-exceeded-attempt
                                2)))
              (list 'cl-resilience-kit:attempt-timeout
                    (list :operation :attempt
                          :deadline 10
                          :observed-at 11
                          :stage :attempt
                          :attempt 1
                          :timeout 0.5d0)
                    (list (list #'cl-resilience-kit:attempt-timeout-timeout
                                0.5d0)))
              (list 'cl-resilience-kit:circuit-open
                    (list :operation :breaker
                          :state :half-open
                          :retry-at 12
                          :generation 3)
                    (list (list #'cl-resilience-kit:circuit-open-state
                                :half-open)
                          (list #'cl-resilience-kit:circuit-open-retry-at
                                12)
                          (list #'cl-resilience-kit:circuit-open-generation
                                3)))
              (list 'cl-resilience-kit:bulkhead-rejected
                    (list :operation :bulkhead :limit 4 :in-flight 4)
                    (list (list #'cl-resilience-kit:bulkhead-rejected-limit
                                4)
                          (list #'cl-resilience-kit:bulkhead-rejected-in-flight
                                4)))
              (list 'cl-resilience-kit:rate-limit-exceeded
                    (list :operation :rate-limit
                          :requested-tokens 3
                          :available-tokens 1
                          :retry-after 2.0d0)
                    (list (list #'cl-resilience-kit:rate-limit-exceeded-requested-tokens
                                3)
                          (list #'cl-resilience-kit:rate-limit-exceeded-available-tokens
                                1)
                          (list #'cl-resilience-kit:rate-limit-exceeded-retry-after
                                2.0d0)))
              (list 'cl-resilience-kit:resilience-store-error
                    (list :operation :store :key key :cause cause)
                    (list (list #'cl-resilience-kit:resilience-store-error-key
                                key)
                          (list #'cl-resilience-kit:resilience-store-error-cause
                                cause)))
              (list 'cl-resilience-kit:resilience-store-conflict
                    (list :operation :store
                          :key key
                          :cause cause
                          :expected-version 1
                          :actual-version 2)
                    (list (list #'cl-resilience-kit:resilience-store-conflict-expected-version
                                1)
                          (list #'cl-resilience-kit:resilience-store-conflict-actual-version
                                2)))
              (list 'cl-resilience-kit:resilience-lease-unavailable
                    (list :operation :lease
                          :key key
                          :owner owner
                          :retry-after 3.0d0)
                    (list (list #'cl-resilience-kit:resilience-lease-unavailable-key
                                key)
                          (list #'cl-resilience-kit:resilience-lease-unavailable-owner
                                owner)
                          (list #'cl-resilience-kit:resilience-lease-unavailable-retry-after
                                3.0d0)))
              (list 'cl-resilience-kit:resilience-lease-lost
                    (list :operation :lease
                          :key key
                          :owner owner
                          :fencing-token 4)
                    (list (list #'cl-resilience-kit:resilience-lease-lost-key
                                key)
                          (list #'cl-resilience-kit:resilience-lease-lost-owner
                                owner)
                          (list #'cl-resilience-kit:resilience-lease-lost-fencing-token
                                4)))
              (list 'cl-resilience-kit:stale-fencing-token
                    (list :operation :lease
                          :key key
                          :fencing-token 4
                          :current-fencing-token 5)
                    (list (list #'cl-resilience-kit:stale-fencing-token-key
                                key)
                          (list #'cl-resilience-kit:stale-fencing-token-fencing-token
                                4)
                          (list #'cl-resilience-kit:stale-fencing-token-current-fencing-token
                                5)))
              (list 'cl-resilience-kit:resilience-draining
                    (list :operation :lifecycle :state :draining)
                    (list (list #'cl-resilience-kit:resilience-draining-state
                                :draining)))
              (list 'cl-resilience-kit:resilience-execution-rejected
                    (list :operation :executor
                          :reason :queue-full
                          :queue-size 3)
                    (list (list #'cl-resilience-kit:resilience-execution-rejected-reason
                                :queue-full)
                          (list #'cl-resilience-kit:resilience-execution-rejected-queue-size
                                3)))
              (list 'cl-resilience-kit:resilience-execution-timeout
                    (list :operation :executor :timeout 2.0d0 :backend :thread)
                    (list (list #'cl-resilience-kit:resilience-execution-timeout-timeout
                                2.0d0)
                          (list #'cl-resilience-kit:resilience-execution-timeout-backend
                                :thread)))
              (list 'cl-resilience-kit:resilience-hard-timeout
                    (list :operation :executor
                          :deadline 10
                          :observed-at 11
                          :stage :attempt
                          :attempt 1
                          :timeout 2.0d0
                          :backend :executor)
                    (list (list #'cl-resilience-kit:resilience-hard-timeout-backend
                                :executor)))
              (list 'cl-resilience-kit:hedge-unsafe
                    (list :operation :hedge :reason :unsafe)
                    (list (list #'cl-resilience-kit:hedge-unsafe-reason
                                :unsafe)))
              (list 'cl-resilience-kit:hedge-exhausted
                    (list :operation :hedge :causes cause :attempts 2)
                    (list (list #'cl-resilience-kit:hedge-exhausted-causes
                                cause)
                          (list #'cl-resilience-kit:hedge-exhausted-attempts
                                2)))
              (list 'cl-resilience-kit:idempotency-key-required
                    (list :operation :idempotency)
                    nil)
              (list 'cl-resilience-kit:idempotency-conflict
                    (list :operation :idempotency
                          :key key
                          :existing-value value)
                    (list (list #'cl-resilience-kit:idempotency-conflict-key
                                key)
                          (list #'cl-resilience-kit:idempotency-conflict-existing-value
                                value))))))
      (dolist (specification specifications)
        (destructuring-bind (type initargs fields) specification
          (let ((condition (apply #'make-condition type initargs)))
            (expect (typep condition type) :to-be-truthy)
            (dolist (field fields)
              (destructuring-bind (reader expected) field
                (expect (funcall reader condition) :to-be expected))))))))
)

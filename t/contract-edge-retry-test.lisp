(in-package #:resilience-kit/test)

(describe "retry and storage edge contracts"
  (it "normalizes classifier retry decisions"
    (let ((policy
            (resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t
             :condition-classifier
             (lambda (condition attempt)
               (declare (ignore condition attempt))
               (resilience-kit:make-retry-decision
                :retry-p t
                :delay-hint 4
                :reason :transient)))))
      (multiple-value-bind (retry-p decision)
          (resilience-kit:retry-policy-should-retry-p
           policy 1 :condition (make-condition 'error))
        (expect retry-p :to-be-truthy)
        (expect (resilience-kit:retry-decision-retry-p decision)
                :to-be-truthy)
        (expect (resilience-kit:retry-decision-delay-hint decision)
                :to-be
                4)
        (expect (resilience-kit:retry-decision-reason decision)
                :to-be
                :transient))))

  (it "validates retry policy inputs and classifier boundaries"
    (expect-invalid-calls (#'resilience-kit:make-retry-policy)
      (:max-attempts 0)
      (:initial-delay -1)
      (:multiplier 0)
      (:max-delay -1)
      (:jitter :unknown))
    (let ((policy (resilience-kit:make-retry-policy)))
      (multiple-value-bind (retry-p decision)
          (resilience-kit:retry-policy-should-retry-p
           policy 1 :condition (make-condition 'error))
        (expect retry-p :to-be nil)
        (expect (resilience-kit:retry-decision-reason decision)
                :to-be
                :not-retry-safe))
      (expect-error-forms
       (resilience-kit:retry-policy-should-retry-p
        policy 0 :result :ok)
       (resilience-kit:retry-policy-should-retry-p policy 1)))
    (let ((policy
            (resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t)))
      (multiple-value-bind (retry-p decision)
          (resilience-kit:retry-policy-should-retry-p
           policy 1 :result nil)
        (expect retry-p :to-be nil)
        (expect (resilience-kit:retry-decision-reason decision)
                :to-be
                :no-classifier)))
    (let ((policy
            (resilience-kit:make-retry-policy
             :max-attempts 2
             :retry-safe-p t
             :result-classifier
             (lambda (result attempt)
               (declare (ignore attempt))
               (if (eq result :retryable) 2 nil)))))
      (multiple-value-bind (retry-p decision)
          (resilience-kit:retry-policy-should-retry-p
           policy 1 :result :retryable)
        (expect retry-p :to-be-truthy)
        (expect (resilience-kit:retry-decision-delay-hint decision)
                :to-be
                2))
      (expect (resilience-kit:compute-backoff-delay
               (resilience-kit:make-retry-policy
                :initial-delay 3
                :multiplier 1)
               100)
              :to-be
              3d0)))

  (it "enforces compare-and-set and lease renewal boundaries"
    (let ((store (resilience-kit:make-memory-state-store)))
      (expect (resilience-kit:state-store-put-if-version
               store "state/a" (list :value 1) nil)
              :to-be
              1)
      (expect (resilience-kit:state-store-put-if-version
               store "state/b" (list :value 2) nil)
              :to-be
              1)
      (let ((condition
              (expect-condition
               (lambda ()
                 (resilience-kit:state-store-put-if-version
                  store "state/a" (list :value 3) nil))
               'resilience-kit:resilience-store-conflict)))
        (expect (resilience-kit:resilience-store-conflict-expected-version
                 condition)
                :to-be
                nil)
        (expect (resilience-kit:resilience-store-conflict-actual-version
                 condition)
                :to-be
                1))
      (let ((condition
              (expect-condition
               (lambda ()
                 (resilience-kit:state-store-delete-if-version
                  store "state/b" 2))
               'resilience-kit:resilience-store-conflict)))
        (expect (resilience-kit:resilience-store-conflict-actual-version
                 condition)
                :to-be
                1))
      (expect (length
               (resilience-kit:state-store-scan-prefix store "state/"))
              :to-be
              2)
      (expect (resilience-kit:state-store-put-if-version
               store "other/a" (list :value 4) nil)
              :to-be
              1)
      (expect (length
               (resilience-kit:state-store-scan-prefix store "state/"))
              :to-be
              2)
      (expect (resilience-kit:state-store-delete-if-version
               store "state/a" 1)
              :to-be-truthy)
      (multiple-value-bind (value version)
          (resilience-kit:state-store-get store "state/a")
        (expect value :to-be nil)
        (expect version :to-be nil))
      (expect-condition
       (lambda ()
         (resilience-kit:state-store-scan-prefix store 1))
       'type-error))
    (let* ((fixture (make-test-fixture :start 10))
           (store
             (resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (first-lease
             (resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 2)))
      (expect (resilience-kit:renew-resilience-lease
               first-lease :ttl 5)
              :to-be
              first-lease)
      (expect (resilience-kit:resilience-lease-ttl first-lease)
              :to-be
              5d0)
      (expect (approximately-equal-p
               (resilience-kit:resilience-lease-expires-at first-lease)
               15)
              :to-be-truthy)
      (advance-fixture fixture 6)
      (expect (resilience-kit:resilience-lease-held-p first-lease)
              :to-be
              nil)
      (expect-condition-form
       (resilience-kit:renew-resilience-lease first-lease)
       'resilience-kit:resilience-lease-lost)
      (let ((second-lease
              (resilience-kit:acquire-resilience-lease
               store "lease/a" "owner-b" :ttl 3)))
        (expect (> (resilience-kit:resilience-lease-fencing-token
                    second-lease)
                   (resilience-kit:resilience-lease-fencing-token
                    first-lease))
                :to-be-truthy)
        (expect (resilience-kit:release-resilience-lease
                 first-lease :ignore-lost-p t)
                :to-be
                nil)
        (expect-condition-form
         (resilience-kit:release-resilience-lease first-lease)
         'resilience-kit:resilience-lease-lost)
        (expect (resilience-kit:release-resilience-lease second-lease)
                :to-be-truthy))
      (let ((inside nil))
        (resilience-kit:with-resilience-lease
            (lease store "lease/macro" "macro-owner" :ttl 2)
          (setf inside
                (resilience-kit:resilience-lease-held-p lease)))
        (expect inside :to-be-truthy)))))

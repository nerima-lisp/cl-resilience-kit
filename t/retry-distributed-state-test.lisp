(in-package #:resilience-kit/test)

(describe "distributed state and budgets"
  (it "provides versioned state-store compare-and-set operations"
    (let ((store (resilience-kit:make-memory-state-store)))
      (expect
       (resilience-kit:state-store-put-if-version
        store "retry/a" '(:value 1) nil)
       :to-be
       1)
      (multiple-value-bind (value version)
          (resilience-kit:state-store-get store "retry/a")
        (expect value :to-equal '(:value 1))
        (expect version :to-be 1))
      (let ((caught
              (expect-condition
               (lambda ()
                 (resilience-kit:state-store-put-if-version
                  store "retry/a" '(:value 2) nil))
               'resilience-kit:resilience-store-conflict)))
        (expect
         (resilience-kit:resilience-store-conflict-expected-version caught)
         :to-be
         nil)
        (expect
         (resilience-kit:resilience-store-conflict-actual-version caught)
         :to-be
         1))
      (expect
       (resilience-kit:state-store-put-if-version
        store "retry/a" '(:value 2) 1)
       :to-be
       2)
      (expect
       (length (resilience-kit:state-store-scan-prefix store "retry/"))
       :to-be
       1)
      (expect
       (resilience-kit:state-store-delete-if-version store "retry/a" 2)
       :to-be-truthy)
      (multiple-value-bind (value version)
          (resilience-kit:state-store-get store "retry/a")
        (expect value :to-be nil)
        (expect version :to-be nil))))

  (it "shares retry budget state across distributed budget instances"
    (let* ((fixture (make-test-fixture))
           (store (resilience-kit:make-memory-state-store))
           (first-budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/shared"
              :limit 2
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (second-budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/shared"
              :limit 2
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect (retry-budget-used first-budget) :to-be 0)
      (expect (retry-budget-acquire first-budget) :to-be-truthy)
      (expect (retry-budget-used second-budget) :to-be 1)
      (expect (retry-budget-acquire second-budget) :to-be-truthy)
      (expect (retry-budget-acquire first-budget) :to-be nil)
      (expect (retry-budget-remaining second-budget) :to-be 0)
      (advance-fixture fixture 10)
      (expect (retry-budget-acquire second-budget) :to-be-truthy)
      (expect (retry-budget-used first-budget) :to-be 1))))

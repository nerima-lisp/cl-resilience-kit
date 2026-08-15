(in-package #:resilience-kit/test)

(defclass always-conflicting-state-store
    (resilience-kit:resilience-state-store)
  ())

(defmethod resilience-kit:state-store-get
    ((store always-conflicting-state-store) key)
  (declare (ignore store key))
  (values nil nil))

(defmethod resilience-kit:state-store-put-if-version
    ((store always-conflicting-state-store) key value expected-version)
  (declare (ignore store value))
  (error 'resilience-kit:resilience-store-conflict
         :key key
         :expected-version expected-version
         :actual-version expected-version))

(describe "retry budgets"
  (it "limits retry authorization with a fixed-window budget"
    (let* ((fixture (make-test-fixture))
           (budget (make-retry-budget
                    :limit 2
                    :window 10
                    :clock (test-fixture-clock fixture)
                    :monotonic-units-per-second
                    +test-monotonic-units-per-second+)))
      (expect (retry-budget-used budget) :to-be 0)
      (expect (retry-budget-remaining budget) :to-be 2)
      (expect (retry-budget-acquire budget) :to-be-truthy)
      (expect (retry-budget-acquire budget) :to-be-truthy)
      (expect (retry-budget-acquire budget) :to-be nil)
      (expect (retry-budget-used budget) :to-be 2)
      (advance-fixture fixture 10)
      (expect (retry-budget-remaining budget) :to-be 2)
      (expect (retry-budget-acquire budget) :to-be-truthy)
      (expect (retry-budget-used budget) :to-be 1)))

  (it "stops retries when the shared budget is exhausted"
    (with-test-fixture (fixture)
      (let* ((budget (make-retry-budget
                      :limit 1
                      :window 10
                      :clock (test-fixture-clock fixture)
                      :monotonic-units-per-second
                      +test-monotonic-units-per-second+))
             (attempts 0)
             (policy (make-retry-policy
                      :max-attempts 5
                      :initial-delay 0
                      :retry-safe-p t
                      :condition-classifier
                      (lambda (condition attempt)
                        (declare (ignore condition attempt))
                        t)))
             (caught (expect-condition
                      (lambda ()
                        (call-with-test-retry
                         fixture
                         policy
                         (lambda ()
                           (incf attempts)
                           (error "transient"))
                         :retry-budget budget))
                      'retry-exhausted)))
        (expect attempts :to-be 2)
        (expect (retry-budget-used budget) :to-be 1)
        (expect (retry-exhausted-reason caught)
                :to-be
                :retry-budget-exhausted)
        (expect (retry-exhausted-attempts caught) :to-be 2))))

  (it-each
      ((:local-limit
        (lambda (store)
          (declare (ignore store))
          (make-retry-budget :limit 0 :window 1)))
       (:local-window
        (lambda (store)
          (declare (ignore store))
          (make-retry-budget :limit 1 :window 0)))
       (:distributed-limit
        (lambda (store)
          (resilience-kit:make-distributed-retry-budget
           :store store
           :key "budget/invalid-limit"
           :limit 0
           :window 1)))
       (:distributed-window
        (lambda (store)
          (resilience-kit:make-distributed-retry-budget
           :store store
           :key "budget/invalid-window"
           :limit 1
           :window 0))))
      "rejects invalid retry budget configuration ~S"
      (case-name constructor)
    (let ((store (resilience-kit:make-memory-state-store)))
      (expect (keywordp case-name) :to-be-truthy)
      (expect (typep (expect-condition
                      (lambda ()
                        (funcall constructor store))
                      'error)
                     'error)
              :to-be-truthy)))

  (it "surfaces repeated distributed budget store conflicts"
    (let* ((store (make-instance 'always-conflicting-state-store))
           (budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/conflicts"
              :limit 1
              :window 10))
           (caught
             (expect-condition
              (lambda () (retry-budget-acquire budget))
              'resilience-kit:resilience-store-error)))
      (expect (resilience-kit:resilience-store-error-key caught)
              :to-be
              "budget/conflicts")
      (expect (resilience-kit:resilience-store-error-cause caught)
              :to-be
              nil)))

  (it "reports malformed persisted distributed budget state"
    (let* ((store (resilience-kit:make-memory-state-store))
           (budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/malformed"
              :limit 1
              :window 10)))
      (expect
       (resilience-kit:state-store-put-if-version
        store
        "budget/malformed"
        '(:window-start :invalid :used 0)
        nil)
       :to-be
       1)
      (let ((caught
              (expect-condition
               (lambda () (retry-budget-used budget))
               'resilience-kit:resilience-store-error)))
        (expect (resilience-kit:resilience-store-error-key caught)
                :to-be
                "budget/malformed"))))

  (it "fails closed when a persisted distributed budget's used exceeds its limit"
    (let* ((fixture (make-test-fixture :start 0))
           (store (resilience-kit:make-memory-state-store))
           (budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/over-limit"
              :limit 1
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      ;; USED can only exceed LIMIT via a write that bypassed the CAS
      ;; protocol, so this is treated as store corruption, not skew.
      (expect
       (resilience-kit:state-store-put-if-version
        store
        "budget/over-limit"
        (list :window-start 0 :used 5)
        nil)
       :to-be
       1)
      (let ((caught
              (expect-condition
               (lambda () (retry-budget-acquire budget))
               'resilience-kit:resilience-store-error)))
        (expect (resilience-kit:resilience-store-error-key caught)
                :to-be
                "budget/over-limit"))))

  (it "preserves used when a persisted distributed budget's window-start is in the future"
    (let* ((fixture (make-test-fixture :start 0))
           (store (resilience-kit:make-memory-state-store))
           (budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/future-window-exhausted"
              :limit 1
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      ;; A window-start ahead of now (benign clock skew between writers)
      ;; must not be rejected, and must not be treated as a fresh window
      ;; that would hand out a free token: used stays exhausted.
      (expect
       (resilience-kit:state-store-put-if-version
        store
        "budget/future-window-exhausted"
        (list :window-start 1000000 :used 1)
        nil)
       :to-be
       1)
      (expect (retry-budget-remaining budget) :to-be 0)
      (expect (retry-budget-acquire budget) :to-be nil)))

  (it "clamps a persisted future window-start to now so the window can still expire"
    (let* ((fixture (make-test-fixture :start 0))
           (store (resilience-kit:make-memory-state-store))
           (budget
             (resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/future-window-recovers"
              :limit 1
              :window 10
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (resilience-kit:state-store-put-if-version
        store
        "budget/future-window-recovers"
        (list :window-start 1000000 :used 0)
        nil)
       :to-be
       1)
      ;; The clamp anchors the effective window-start to now (0), not the
      ;; raw future timestamp, so the acquisition is authorized and the
      ;; anchored value is what gets persisted back.
      (expect (retry-budget-acquire budget) :to-be-truthy)
      ;; Advancing past the window from that anchor must free the token
      ;; again. If the clamp were absent, the raw future window-start would
      ;; never come due and this budget would stay exhausted forever.
      (advance-fixture fixture 11)
      (expect (retry-budget-remaining budget) :to-be 1))))

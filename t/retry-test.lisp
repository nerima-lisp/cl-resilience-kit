(in-package #:cl-resilience-kit/test)

(defclass always-conflicting-state-store
    (cl-resilience-kit:resilience-state-store)
  ())

(defmethod cl-resilience-kit:state-store-get
    ((store always-conflicting-state-store) key)
  (declare (ignore store key))
  (values nil nil))

(defmethod cl-resilience-kit:state-store-put-if-version
    ((store always-conflicting-state-store) key value expected-version)
  (declare (ignore store value))
  (error 'cl-resilience-kit:resilience-store-conflict
         :key key
         :expected-version expected-version
         :actual-version expected-version))

(describe "retry policies"
  (it-each
      ((nil nil nil)
       (t nil nil)
       (t t t))
      "requires retry safety and a positive classifier result (~S, ~S)"
      (safe-p classifier-result expected)
    (let ((policy (make-retry-policy
                   :max-attempts 2
                   :retry-safe-p safe-p
                   :result-classifier
                   (lambda (result attempt)
                     (declare (ignore result attempt))
                     classifier-result))))
      (expect (retry-policy-should-retry-p policy 1 :result :value)
              :to-be expected)))

  (it "retries classified conditions up to max-attempts"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 3
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (< attempts 3)
              (error "transient")
              :ok))
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :ok)
      (expect attempts :to-be 3)))

  (it "does not retry an unclassified condition"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (caught nil)
           (policy (make-retry-policy
                    :max-attempts 5
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      nil))))
      (handler-case
          (call-with-retry
           policy
           (lambda ()
             (incf attempts)
             (error "permanent"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (error (condition)
          (setf caught condition)))
      (expect attempts :to-be 1)
      (expect (typep caught 'error) :to-be-truthy)))

  (it "classifies returned results without an HTTP-specific type"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :result-classifier
                    (lambda (result attempt)
                      (declare (ignore attempt))
                      (eq result :retry)))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (= attempts 1) :retry :ok))
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :ok)
      (expect attempts :to-be 2)))

  (it "keeps an unsafe operation single-attempt by default"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (classifier-calls 0)
           (caught nil)
           (policy (make-retry-policy
                    :max-attempts 5
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (incf classifier-calls)
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda ()
             (incf attempts)
             (error "write failed"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (error (condition)
          (setf caught condition)))
      (expect attempts :to-be 1)
      (expect classifier-calls :to-be 0)
      (expect (typep caught 'error) :to-be-truthy)))

  (it "preserves multiple return values on the no-classifier fast path"
    (let* ((fixture (make-test-fixture))
           (policy (make-retry-policy)))
      (multiple-value-bind (value marker absent)
          (call-with-retry
           policy
           (lambda () (values :ok 42 nil))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (expect value :to-be :ok)
        (expect marker :to-be 42)
        (expect absent :to-be nil))))

  (it "signals structured retry exhaustion"
    (let* ((fixture (make-test-fixture))
           (exhausted nil)
           (policy (make-retry-policy
                    :max-attempts 3
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda () (error "always fails"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (retry-exhausted (condition)
          (setf exhausted condition)))
      (expect (typep exhausted 'retry-exhausted) :to-be-truthy)
      (expect (retry-exhausted-attempts exhausted) :to-be 3)
      (expect (typep (retry-exhausted-last-condition exhausted) 'error)
              :to-be-truthy)))

  (it "retains a classifier reason in retry exhaustion"
    (let* ((fixture (make-test-fixture))
           (exhausted nil)
           (policy (make-retry-policy
                    :max-attempts 1
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (make-retry-decision
                       :retry-p t
                       :reason :classified-transient)))))
      (handler-case
          (call-with-retry
           policy
           (lambda () (error "classified failure"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (retry-exhausted (condition)
          (setf exhausted condition)))
      (expect (typep exhausted 'retry-exhausted) :to-be-truthy)
      (expect (retry-exhausted-reason exhausted)
              :to-be
              :classified-transient)))

  (it "records injected sleeps and applies the backoff cap"
    (let* ((fixture (make-test-fixture))
           (exhausted nil)
           (policy (make-retry-policy
                    :max-attempts 3
                    :initial-delay 1
                    :multiplier 2
                    :max-delay 1.5
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda () (error "always fails"))
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (retry-exhausted (condition)
          (setf exhausted condition)))
      (expect (typep exhausted 'retry-exhausted) :to-be-truthy)
      (expect (fixture-sleeps fixture) :to-equal '(1.0d0 1.5d0))
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 10
         :multiplier 3
         :max-delay 15)
        2)
       :to-be
       15.0d0)))

  (it "makes every jitter strategy deterministic with an injected source"
    (let ((fixture (make-test-fixture :random-values '(500000 500000 500000))))
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 10
         :jitter :full
         :random-source (test-fixture-random-source fixture))
        1)
       :to-be
       5.0d0)
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 10
         :jitter :equal
         :random-source (test-fixture-random-source fixture))
        1)
       :to-be
       7.5d0)
      (expect
       (compute-backoff-delay
        (make-retry-policy
         :initial-delay 2
         :jitter :decorrelated
         :random-source (test-fixture-random-source fixture))
        2
        :previous-delay 2)
       :to-be
       4.0d0)
      (expect
       (compute-backoff-delay
        (make-retry-policy :initial-delay 0 :jitter :full)
        1)
       :to-be
       0d0)))

  (it "returns a normalized retry decision without sleeping"
    (let ((policy (make-retry-policy
                   :max-attempts 2
                   :retry-safe-p t
                   :condition-classifier
                   (lambda (condition attempt)
                     (declare (ignore condition attempt))
                     (values t 0.25)))))
      (multiple-value-bind (retry-p decision)
          (retry-policy-should-retry-p
           policy
           1
           :condition (make-condition 'simple-error
                                      :format-control "transient"))
        (expect retry-p :to-be-truthy)
        (expect (typep decision 'cl-resilience-kit:retry-decision)
                :to-be-truthy))))

  (it "provides a block-oriented retry macro"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (with-retry (policy
                    :clock (test-fixture-clock fixture)
                    :monotonic-units-per-second
                    +test-monotonic-units-per-second+
                    :sleeper (test-fixture-sleeper fixture))
         (incf attempts)
         (if (= attempts 1)
             (error "transient")
             :ok))
       :to-be
       :ok)
      (expect attempts :to-be 2)))

  (it-property "backoff never exceeds its configured maximum"
      ((retry-number (gen-integer :min 1 :max 8))
       (maximum (gen-integer :min 0 :max 20)))
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier 2
                   :max-delay maximum)))
      (expect (<= (compute-backoff-delay policy retry-number)
                  (float maximum 1d0))
              :to-be-truthy)))

  (it "supports continuation-oriented test bodies"
    (with-continuation-result (result next calledp)
        ((lambda (continuation)
           (funcall continuation :ok :ignored))
         #'next)
      (expect calledp :to-be-truthy)
      (expect result :to-be :ok)))

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
    (let* ((fixture (make-test-fixture))
           (budget (make-retry-budget
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
                      (call-with-retry
                       policy
                       (lambda ()
                         (incf attempts)
                         (error "transient"))
                       :retry-budget budget
                       :clock (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper (test-fixture-sleeper fixture)))
                    'retry-exhausted)))
      (expect attempts :to-be 2)
      (expect (retry-budget-used budget) :to-be 1)
      (expect (retry-exhausted-reason caught)
              :to-be
              :retry-budget-exhausted)
      (expect (retry-exhausted-attempts caught) :to-be 2)))

  (it "publishes attempt events and tolerates observer failures"
    (let* ((fixture (make-test-fixture))
           (events nil)
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 0
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (= attempts 1)
              (error "transient")
              :ok))
        :operation :read
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second
        +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture)
        :event-handler
        (lambda (event)
          (push event events)
          (when (eq (resilience-event-type event) :attempt-failure)
            (error "observer failure"))))
       :to-be
       :ok)
      (let ((types (mapcar #'resilience-event-type (reverse events))))
        (expect (count :attempt-start types) :to-be 2)
        (expect (count :attempt-failure types) :to-be 1)
        (expect (count :attempt-success types) :to-be 1)
        (expect (count :retry-scheduled types) :to-be 1)
        (expect (every (lambda (event)
                         (and (resilience-event-p event)
                              (eq (resilience-event-operation event) :read)))
                       events)
                :to-be-truthy))))

  (it "invokes a fallback when retry exhaustion is terminal"
    (let* ((fixture (make-test-fixture))
           (fallback-condition nil)
           (policy (make-retry-policy
                    :max-attempts 1
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (expect
       (call-with-retry
        policy
        (lambda () (error "permanent"))
        :fallback (lambda (condition)
                    (setf fallback-condition condition)
                    :fallback)
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second
        +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :fallback)
      (expect (typep fallback-condition 'retry-exhausted)
              :to-be-truthy)
      (expect (retry-exhausted-attempts fallback-condition) :to-be 1)))

  (it "surfaces classifier failures as public resilience conditions"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 3
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      (error "classifier failure"))))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda ()
                         (incf attempts)
                         (error "operation failure"))
                       :clock (test-fixture-clock fixture)
                       :monotonic-units-per-second
                       +test-monotonic-units-per-second+
                       :sleeper (test-fixture-sleeper fixture)))
                    'retry-classifier-error)))
      (expect attempts :to-be 1)
      (expect (typep (retry-classifier-error-cause caught) 'error)
              :to-be-truthy)))

  (it "applies numeric result classifier delay hints during execution"
    (let* ((fixture (make-test-fixture))
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 1
                    :retry-safe-p t
                    :result-classifier
                    (lambda (result attempt)
                      (declare (ignore result))
                      (when (= attempt 1) 5)))))
      (expect
       (call-with-retry
        policy
        (lambda ()
          (incf attempts)
          (if (= attempts 1) :retryable :ok))
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second +test-monotonic-units-per-second+
        :sleeper (test-fixture-sleeper fixture))
       :to-be
       :ok)
      (expect (fixture-sleeps fixture) :to-equal '(5.0d0))
      (expect attempts :to-be 2)))

  (it "keeps exponential backoff finite under numeric overflow pressure"
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier most-positive-double-float)))
      (let ((delay (compute-backoff-delay policy 2)))
        (expect (floatp delay) :to-be-truthy)
        (expect (<= 0d0 delay most-positive-double-float)
                :to-be-truthy)))))

  (it "caps very large retry numbers without a linear delay loop"
    (let ((policy (make-retry-policy
                   :initial-delay 1
                   :multiplier 2
                   :max-delay 10)))
           (expect (compute-backoff-delay policy most-positive-fixnum)
              :to-be
              10d0)))

  (it "rejects invalid local and distributed budget configuration"
    (let ((store (cl-resilience-kit:make-memory-state-store)))
      (expect-condition
       (lambda () (make-retry-budget :limit 0 :window 1))
       'error)
      (expect-condition
       (lambda () (make-retry-budget :limit 1 :window 0))
       'error)
      (expect-condition
       (lambda ()
         (cl-resilience-kit:make-distributed-retry-budget
          :store store
          :key "budget/invalid-limit"
          :limit 0
          :window 1))
       'error)
       (expect-condition
        (lambda ()
         (cl-resilience-kit:make-distributed-retry-budget
          :store store
          :key "budget/invalid-window"
          :limit 1
          :window 0))
       'error)))

  (it "surfaces repeated distributed budget store conflicts"
    (let* ((store (make-instance 'always-conflicting-state-store))
           (budget
             (cl-resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/conflicts"
              :limit 1
              :window 10))
           (caught
             (expect-condition
              (lambda () (retry-budget-acquire budget))
              'cl-resilience-kit:resilience-store-error)))
      (expect (cl-resilience-kit:resilience-store-error-key caught)
              :to-be
              "budget/conflicts")
      (expect (cl-resilience-kit:resilience-store-error-cause caught)
              :to-be
              nil)))

  (it "reports malformed persisted distributed budget state"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (budget
             (cl-resilience-kit:make-distributed-retry-budget
              :store store
              :key "budget/malformed"
              :limit 1
              :window 10)))
      (expect
       (cl-resilience-kit:state-store-put-if-version
        store
        "budget/malformed"
        '(:window-start :invalid :used 0)
        nil)
       :to-be
       1)
      (let ((caught
              (expect-condition
               (lambda () (retry-budget-used budget))
               'cl-resilience-kit:resilience-store-error)))
        (expect (cl-resilience-kit:resilience-store-error-key caught)
                :to-be
                "budget/malformed"))))

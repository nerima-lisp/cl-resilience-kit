(in-package #:cl-resilience-kit/test)

(describe "deadlines and attempt timeouts"
  (it "uses monotonic fake time for a cooperative deadline"
    (let* ((fixture (make-test-fixture))
           (caught nil))
      (handler-case
          (call-with-deadline
           (lambda ()
             (advance-fixture fixture 2)
             :finished)
           :timeout 1
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+)
        (deadline-exceeded (condition)
          (setf caught condition)))
      (expect (typep caught 'deadline-exceeded) :to-be-truthy)
      (expect (deadline-exceeded-stage caught) :to-be :operation)
      (expect (deadline-exceeded-attempt caught) :to-be nil)))

  (it "exposes remaining time inside the deadline context"
    (let ((fixture (make-test-fixture)))
      (multiple-value-bind (deadline remaining exceeded-p)
          (call-with-deadline
           (lambda ()
             (let ((remaining (deadline-remaining
                               :clock (test-fixture-clock fixture))))
               (advance-fixture fixture 0.25)
               (values (current-deadline)
                       remaining
                       (deadline-exceeded-p
                        :clock (test-fixture-clock fixture)))))
           :timeout 1
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+)
        (expect deadline :to-be 1.0d0)
        (expect remaining :to-be 1.0d0)
        (expect exceeded-p :to-be nil))))

  (it "rejects mixing relative and absolute deadlines"
    (expect
     (lambda ()
       (call-with-deadline (lambda () :ok)
                            :timeout 1
                            :deadline 2))
     :to-throw
     'error))

  (it "rejects a non-finite absolute deadline"
    (expect-condition
     (lambda ()
       (call-with-deadline (lambda () :never)
                            :deadline :invalid))
     'error))

  (it "accepts an absolute monotonic deadline"
    (let ((fixture (make-test-fixture :start 2)))
      (multiple-value-bind (result deadline)
          (call-with-deadline
           (lambda () (values :ok (current-deadline)))
           :deadline 5
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+)
        (expect result :to-be :ok)
        (expect deadline :to-be 5.0d0))))

  (it "rejects an already expired absolute deadline before invocation"
    (let* ((fixture (make-test-fixture :start 2))
           (attempts 0)
           (caught
             (expect-condition
              (lambda ()
                (call-with-deadline
                 (lambda ()
                   (incf attempts)
                   :not-run)
                 :deadline 1
                 :clock (test-fixture-clock fixture)
                 :monotonic-units-per-second
                 +test-monotonic-units-per-second+))
              'deadline-exceeded)))
      (expect attempts :to-be 0)
      (expect (deadline-exceeded-stage caught) :to-be :before-operation)))

  (it "distinguishes overall deadline from per-attempt timeout"
    (let* ((fixture (make-test-fixture))
           (caught nil)
           (policy (make-retry-policy :max-attempts 1 :retry-safe-p t)))
      (handler-case
          (call-with-retry
           policy
           (lambda ()
             (advance-fixture fixture 1)
             :finished)
           :per-attempt-timeout 1
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (attempt-timeout (condition)
          (setf caught condition)))
      (expect (typep caught 'attempt-timeout) :to-be-truthy)
      (expect (attempt-timeout-timeout caught) :to-be 1.0d0)))

  (it "does not sleep a backoff that would cross the overall deadline"
    (let* ((fixture (make-test-fixture))
           (caught nil)
           (attempts 0)
           (policy (make-retry-policy
                    :max-attempts 2
                    :initial-delay 1
                    :retry-safe-p t
                    :condition-classifier
                    (lambda (condition attempt)
                      (declare (ignore condition attempt))
                      t))))
      (handler-case
          (call-with-retry
           policy
           (lambda ()
             (incf attempts)
             (error "transient"))
           :overall-timeout 1
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second +test-monotonic-units-per-second+
           :sleeper (test-fixture-sleeper fixture))
        (deadline-exceeded (condition)
          (setf caught condition)))
      (expect attempts :to-be 1)
      (expect (deadline-exceeded-stage caught) :to-be :backoff)
      (expect (fixture-sleeps fixture) :to-equal nil)))

  (it "documents cooperative timeout behavior through the public condition"
    (let* ((fixture (make-test-fixture))
           (policy (make-retry-policy :max-attempts 1 :retry-safe-p t))
           (caught (expect-condition
                    (lambda ()
                      (call-with-retry
                       policy
                       (lambda ()
                         (advance-fixture fixture 2)
                         :finished)
                       :overall-timeout 1
                       :clock (test-fixture-clock fixture)
                       :monotonic-units-per-second +test-monotonic-units-per-second+
                       :sleeper (test-fixture-sleeper fixture)))
                    'deadline-exceeded)))
      (expect (typep caught 'deadline-exceeded) :to-be-truthy)))

  (it "propagates parent cancellation before invoking an operation"
    (let* ((parent (make-cancellation-token))
           (child (make-cancellation-token :parent parent))
           (attempts 0))
      (cancel-cancellation-token parent :shutdown)
      (let ((caught (expect-condition
                     (lambda ()
                       (call-with-deadline
                        (lambda ()
                          (incf attempts)
                          :not-run)
                        :timeout 10
                        :cancellation-token child
                        :operation :read))
                     'resilience-cancelled)))
        (expect attempts :to-be 0)
        (expect (resilience-cancelled-reason caught) :to-be :shutdown)
        (expect (cancellation-token-cancelled-p child)
                :to-be-truthy))))

  (it "observes cancellation after a cooperative operation returns"
    (let* ((token (make-cancellation-token))
           (caught (expect-condition
                    (lambda ()
                      (call-with-deadline
                       (lambda ()
                         (cancel-cancellation-token token :completed-elsewhere)
                         :returned)
                       :timeout 10
                       :cancellation-token token))
                    'resilience-cancelled)))
      (expect (resilience-cancelled-reason caught) :to-be :completed-elsewhere))))

  (it "rejects malformed cancellation arguments"
    (let ((token (make-cancellation-token)))
      (expect-condition
       (lambda ()
         (cancel-cancellation-token token :first :second))
       'error)
      (expect (cancellation-token-cancelled-p token) :to-be nil)))

  (it "preserves an operation error while unwinding"
    (let ((token (make-cancellation-token)))
      (expect-condition
       (lambda ()
       (call-with-deadline
          (lambda ()
            (cancel-cancellation-token token :during-failure)
            (error "original"))
          :timeout 10
          :cancellation-token token))
       'simple-error))

  (it "does not extend a parent deadline in nested scopes"
    (let ((fixture (make-test-fixture)))
      (expect
       (call-with-deadline
        (lambda ()
          (call-with-deadline
           (lambda () (current-deadline))
           :timeout 10
           :clock (test-fixture-clock fixture)
           :monotonic-units-per-second
           +test-monotonic-units-per-second+))
        :timeout 1
        :clock (test-fixture-clock fixture)
        :monotonic-units-per-second
        +test-monotonic-units-per-second+)
       :to-be
       1.0d0)))

  (it "rejects non-positive monotonic units"
    (expect-condition
     (lambda ()
       (call-with-deadline
        (lambda () :never)
        :timeout 1
        :monotonic-units-per-second 0))
     'error)))

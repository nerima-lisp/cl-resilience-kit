(in-package #:resilience-kit/test)

(describe "retry policy contracts"
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
    (with-test-condition-retry-policy
        (fixture
         policy
         :max-attempts 3
         :initial-delay 0)
      (let ((attempts 0))
        (expect
         (call-with-test-retry
          fixture
          policy
          (lambda ()
            (incf attempts)
            (if (< attempts 3)
                (error "transient")
                :ok)))
         :to-be
         :ok)
        (expect attempts :to-be 3))))

  (it "does not retry an unclassified condition"
    (with-test-retry-policy
        (fixture
         policy
         :max-attempts 5
         :retry-safe-p t
         :condition-classifier
         (lambda (condition attempt)
           (declare (ignore condition attempt))
           nil))
      (let* ((attempts 0)
             (caught (expect-condition
                      (lambda ()
                        (call-with-test-retry
                         fixture
                         policy
                         (lambda ()
                           (incf attempts)
                           (error "permanent"))))
                      'error)))
        (expect attempts :to-be 1)
        (expect (typep caught 'error) :to-be-truthy))))

  (it "classifies returned results without an HTTP-specific type"
    (with-test-retry-policy
        (fixture
         policy
         :max-attempts 2
         :initial-delay 0
         :retry-safe-p t
         :result-classifier
         (lambda (result attempt)
           (declare (ignore attempt))
           (eq result :retry)))
      (let ((attempts 0))
        (expect
         (call-with-test-retry
          fixture
          policy
          (lambda ()
            (incf attempts)
            (if (= attempts 1) :retry :ok)))
         :to-be
         :ok)
        (expect attempts :to-be 2))))

  (it "keeps an unsafe operation single-attempt by default"
    (let ((classifier-calls 0))
      (with-test-retry-policy
          (fixture
           policy
           :max-attempts 5
           :condition-classifier
           (lambda (condition attempt)
             (declare (ignore condition attempt))
             (incf classifier-calls)
             t))
        (let* ((attempts 0)
               (caught (expect-condition
                        (lambda ()
                          (call-with-test-retry
                           fixture
                           policy
                           (lambda ()
                             (incf attempts)
                             (error "write failed"))))
                        'error)))
          (expect attempts :to-be 1)
          (expect classifier-calls :to-be 0)
          (expect (typep caught 'error) :to-be-truthy)))))

  (it "preserves multiple return values on the no-classifier fast path"
    (with-test-fixture (fixture)
      (let ((policy (make-retry-policy)))
        (multiple-value-bind (value marker absent)
            (call-with-test-retry
             fixture
             policy
             (lambda () (values :ok 42 nil)))
          (expect value :to-be :ok)
          (expect marker :to-be 42)
          (expect absent :to-be nil)))))

  (it "signals structured retry exhaustion"
    (with-test-condition-retry-policy
        (fixture
         policy
         :max-attempts 3
         :initial-delay 0)
      (let ((exhausted
              (expect-test-retry-exhausted
               (fixture
                policy
                (lambda () (error "always fails"))))))
        (expect (retry-exhausted-attempts exhausted) :to-be 3)
        (expect (typep (retry-exhausted-last-condition exhausted) 'error)
                :to-be-truthy))))

  (it "retains a classifier reason in retry exhaustion"
    (with-test-retry-policy
        (fixture
         policy
         :max-attempts 1
         :retry-safe-p t
         :condition-classifier
         (lambda (condition attempt)
           (declare (ignore condition attempt))
           (make-retry-decision
            :retry-p t
            :reason :classified-transient)))
      (let ((exhausted
              (expect-test-retry-exhausted
               (fixture
                policy
                (lambda () (error "classified failure"))))))
        (expect (retry-exhausted-reason exhausted)
                :to-be
                :classified-transient))))

  (it "records injected sleeps and applies the backoff cap"
    (with-test-condition-retry-policy
        (fixture
         policy
         :max-attempts 3
         :initial-delay 1
         :multiplier 2
         :max-delay 1.5)
      (expect-test-retry-exhausted
       (fixture
        policy
        (lambda () (error "always fails"))))
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

  (it-each
      ((:full 10 1 nil 5.0d0 t)
       (:equal 10 1 nil 7.5d0 t)
       (:decorrelated 2 2 2 4.0d0 t)
       (:full 0 1 nil 0d0 nil))
      "makes jitter strategy ~S deterministic with an injected source"
      (jitter initial-delay retry-number previous-delay expected
       inject-random-source-p)
    (let ((fixture (make-test-fixture :random-values '(500000 500000 500000))))
      (expect
       (compute-backoff-delay
        (apply #'make-retry-policy
               :initial-delay initial-delay
               :jitter jitter
               (when inject-random-source-p
                 (list :random-source
                       (test-fixture-random-source fixture))))
        retry-number
        :previous-delay previous-delay)
       :to-be
       expected)))

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
        (expect (typep decision 'resilience-kit:retry-decision)
                :to-be-truthy))))

  (it "provides a block-oriented retry macro"
    (with-test-condition-retry-policy
        (fixture
         policy
         :max-attempts 2
         :initial-delay 0)
      (let ((attempts 0))
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
        (expect attempts :to-be 2))))

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
      (expect result :to-be :ok))))

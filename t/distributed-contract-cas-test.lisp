(in-package #:resilience-kit/test)

(describe "distributed and asynchronous boundary contracts"
  (it "retries fenced distributed transitions after CAS conflicts"
    (let* ((delegate (resilience-kit:make-memory-state-store))
           (key "open-transition-conflict")
           (fixture (make-test-fixture :start 1))
           (store
             (make-instance
              'controlled-conflict-state-store
              :delegate delegate
              :remaining-conflicts 1))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :reset-timeout 1
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (seed-distributed-state
       delegate key :state :open :opened-at 0 :generation 3)
      (expect (resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :recovered))
              :to-be
              :recovered)
      (expect (controlled-conflict-state-store-remaining-conflicts store)
              :to-be
              0)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:distributed-circuit-breaker-generation breaker)
              :to-be
              5)))

  (it "retries half-open probe reservations after CAS conflicts"
    (let* ((delegate (resilience-kit:make-memory-state-store))
           (key "half-open-transition-conflict")
           (store
             (make-instance
              'controlled-conflict-state-store
              :delegate delegate
              :remaining-conflicts 1))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :half-open-probe-limit 2)))
      (seed-distributed-state
       delegate key :state :half-open :active-probes 0 :generation 7)
      (expect (resilience-kit:distributed-circuit-breaker-call
               breaker (lambda () :recovered))
              :to-be
              :recovered)
      (expect (controlled-conflict-state-store-remaining-conflicts store)
              :to-be
              0)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)
      (expect (resilience-kit:distributed-circuit-breaker-generation breaker)
              :to-be
              8)))

  (it "reports an error after call-reservation CAS exhaustion"
    (let* ((delegate (resilience-kit:make-memory-state-store))
           (key "call-reservation-conflicts")
           (store
             (make-instance
              'controlled-conflict-state-store
              :delegate delegate
              :remaining-conflicts 64))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key)))
      (seed-distributed-state delegate key)
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () :unreachable)))
       'resilience-kit:resilience-store-error)
      (expect (controlled-conflict-state-store-remaining-conflicts store)
              :to-be
              0)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :closed)))

  (it "ignores stale distributed completion tokens"
    (let ((store (resilience-kit:make-memory-state-store)))
      (dolist (entry '((:generation . 99) (:state . :open)))
        (let* ((field (car entry))
               (value (cdr entry))
               (key (format nil "stale-token-~A" field))
               (breaker
                 (resilience-kit:make-distributed-circuit-breaker
                  :store store
                  :key key)))
          (seed-distributed-state store key)
          (expect
           (resilience-kit:distributed-circuit-breaker-call
            breaker
            (lambda ()
              (multiple-value-bind (state version)
                  (resilience-kit:state-store-get store key)
                (setf (getf state field) value)
                (when (eq field :state)
                  (setf (getf state :opened-at) 0))
                (resilience-kit:state-store-put-if-version
                 store key state version)
                :stale)))
           :to-be
           :stale)
          (expect (resilience-kit:distributed-circuit-breaker-state breaker)
                  :to-be
                  (if (eq field :state) :open :closed))))))

  (it "records a failure after completion CAS exhaustion"
    (let* ((delegate (resilience-kit:make-memory-state-store))
           (key "completion-conflicts")
           (store
             (make-instance
              'controlled-conflict-state-store
              :delegate delegate
              :remaining-conflicts 64))
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :failure-threshold 1)))
      (seed-distributed-state delegate key)
      (expect-condition
       (lambda ()
         (resilience-kit:distributed-circuit-breaker-call
          breaker (lambda () :completion-failure)))
       'resilience-kit:resilience-store-error)
      (expect (controlled-conflict-state-store-remaining-conflicts store)
              :to-be
              0)
      (expect (resilience-kit:distributed-circuit-breaker-state breaker)
              :to-be
              :open))))

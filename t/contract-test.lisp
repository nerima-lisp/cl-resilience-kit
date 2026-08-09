(in-package #:cl-resilience-kit/test)

(defclass controlled-conflict-state-store
    (cl-resilience-kit:resilience-state-store)
  ((delegate
    :initarg :delegate
    :reader controlled-conflict-state-store-delegate)
   (remaining-conflicts
    :initarg :remaining-conflicts
    :accessor controlled-conflict-state-store-remaining-conflicts)))

(defmethod cl-resilience-kit:state-store-get
    ((store controlled-conflict-state-store) key)
  (cl-resilience-kit:state-store-get
   (controlled-conflict-state-store-delegate store)
   key))

(defmethod cl-resilience-kit:state-store-put-if-version
    ((store controlled-conflict-state-store) key value expected-version)
  (if (plusp (controlled-conflict-state-store-remaining-conflicts store))
      (progn
        (decf (controlled-conflict-state-store-remaining-conflicts store))
        (error 'cl-resilience-kit:resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version expected-version))
      (cl-resilience-kit:state-store-put-if-version
       (controlled-conflict-state-store-delegate store)
       key
       value
       expected-version)))

(defun seed-distributed-state
    (store key &key (state :closed) (failure-count 0) opened-at
          (active-probes 0) (half-open-successes 0) (generation 0))
  (cl-resilience-kit:state-store-put-if-version
   store
   key
   (list :state state
         :failure-count failure-count
         :opened-at opened-at
         :active-probes active-probes
         :half-open-successes half-open-successes
         :generation generation)
   nil))

(describe "public contracts"
  (it "records metrics and isolates observer failures"
    (let* ((metrics (cl-resilience-kit:make-resilience-metrics))
           (event (cl-resilience-kit:make-resilience-event
                    :type :success :operation :read :duration 1.5d0))
           (undated (cl-resilience-kit:make-resilience-event
                      :type :success :operation :read))
           (calls 0)
           (observer
             (cl-resilience-kit:make-resilience-observer
              (lambda (ignored)
                (declare (ignore ignored))
                (error "observer failure"))
              (lambda (ignored)
                (declare (ignore ignored))
                (incf calls)))))
      (expect (cl-resilience-kit:record-resilience-event metrics event)
              :to-be-truthy)
      (cl-resilience-kit:record-resilience-event metrics undated)
      (expect (cl-resilience-kit:resilience-metrics-total-events metrics)
              :to-be 2)
      (expect (cl-resilience-kit:resilience-metrics-count
               metrics :success :operation :read)
              :to-be 2)
      (expect (cl-resilience-kit:resilience-metrics-duration
               metrics :success :operation :read)
              :to-be 1.5d0)
      (let ((snapshot (cl-resilience-kit:resilience-metrics-snapshot metrics)))
        (expect (getf snapshot :total-events) :to-be 2)
        (expect (length (getf snapshot :events)) :to-be 1)
        (expect (length (getf snapshot :durations)) :to-be 1))
      (expect (cl-resilience-kit:resilience-observer-handlers observer)
              :to-be-truthy)
      (let ((returned
              (funcall (cl-resilience-kit:resilience-observer-handler observer)
                       event)))
        (expect returned :to-be-truthy))
      (expect calls :to-be 1)
      (expect (cl-resilience-kit:reset-resilience-metrics metrics)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-metrics-total-events metrics)
              :to-be 0)))

  (it "uses injected clocks for breaker event timestamps"
    (let* ((fixture (make-test-fixture :start 42))
           (events nil)
           (breaker
             (cl-resilience-kit:make-circuit-breaker
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (cl-resilience-kit:circuit-breaker-call
        breaker (lambda () :ok)
        :operation :read
        :event-handler (lambda (event) (push event events)))
       :to-be
       :ok)
      (expect (length events) :to-be 1)
      (expect
       (approximately-equal-p
        (cl-resilience-kit:resilience-event-timestamp (first events))
        42)
       :to-be-truthy))
    (let* ((fixture (make-test-fixture :start 42))
           (store (cl-resilience-kit:make-memory-state-store))
           (events nil)
           (breaker
             (cl-resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "service-a"
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (cl-resilience-kit:distributed-circuit-breaker-call
        breaker (lambda () :ok)
        :operation :read
        :event-handler (lambda (event) (push event events)))
       :to-be
       :ok)
      (expect (length events) :to-be 1)
      (expect
       (approximately-equal-p
        (cl-resilience-kit:resilience-event-timestamp (first events))
        42)
       :to-be-truthy)))

  (it "preserves structured condition data"
    (let ((condition
            (make-condition 'cl-resilience-kit:deadline-exceeded
                            :operation :read
                            :deadline 5d0
                            :observed-at 5d0
                            :stage :wait
                            :attempt 2)))
      (expect (typep condition 'cl-resilience-kit:resilience-error)
              :to-be-truthy)
      (expect (cl-resilience-kit:deadline-exceeded-stage condition)
              :to-be
              :wait)
      (expect (cl-resilience-kit:deadline-exceeded-attempt condition)
              :to-be
              2)
      (expect (stringp (princ-to-string condition)) :to-be-truthy)))

  (it "copies state values and exposes the store protocol error"
    (let* ((store (cl-resilience-kit:make-memory-state-store))
           (value (list :nested (list 1 2)))
           (original (list :nested (list 1 2)))
           (version
             (cl-resilience-kit:state-store-put-if-version
              store "state/a" value nil)))
      (expect version :to-be 1)
      (setf (second (getf value :nested)) 9)
      (multiple-value-bind (loaded loaded-version)
          (cl-resilience-kit:state-store-get store "state/a")
        (expect loaded-version :to-be 1)
        (expect loaded :to-equal original)
        (setf (second (getf loaded :nested)) 8))
      (multiple-value-bind (fresh fresh-version)
          (cl-resilience-kit:state-store-get store "state/a")
        (expect fresh-version :to-be 1)
        (expect fresh :to-equal original))
      (expect (length
               (cl-resilience-kit:state-store-scan-prefix store "state/"))
              :to-be 1)
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:state-store-get
                  (make-instance 'cl-resilience-kit:resilience-state-store)
                  "missing"))
               'cl-resilience-kit:resilience-store-error)))
        (expect condition :to-be-truthy)
        (expect (cl-resilience-kit:resilience-store-error-key condition)
                :to-be
                "missing"))))

  (it "enforces fencing lease ownership and supports soft contention"
    (let* ((fixture (make-test-fixture :start 10))
           (store
             (cl-resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (first-lease
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 10))
           (same-owner
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 10))
           (soft-contention
             (cl-resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-b" :ttl 10
              :signal-on-unavailable-p nil)))
      (expect first-lease :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lease-fencing-token same-owner)
              :to-be
              (cl-resilience-kit:resilience-lease-fencing-token first-lease))
      (expect soft-contention :to-be nil)
      (let ((condition
              (expect-condition
               (lambda ()
                 (cl-resilience-kit:acquire-resilience-lease
                  store "lease/a" "owner-b" :ttl 10))
               'cl-resilience-kit:resilience-lease-unavailable)))
        (expect condition :to-be-truthy)
        (expect (cl-resilience-kit:resilience-lease-unavailable-retry-after
                 condition)
                :to-be-truthy))
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be-truthy)
      (expect (cl-resilience-kit:release-resilience-lease first-lease)
              :to-be-truthy)
      (expect (cl-resilience-kit:resilience-lease-held-p first-lease)
              :to-be
              nil))))

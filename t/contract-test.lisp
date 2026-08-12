(in-package #:resilience-kit/test)

(defclass controlled-conflict-state-store
    (resilience-kit:resilience-state-store)
  ((delegate
    :initarg :delegate
    :reader controlled-conflict-state-store-delegate)
   (remaining-conflicts
    :initarg :remaining-conflicts
    :accessor controlled-conflict-state-store-remaining-conflicts)))

(defmethod resilience-kit:state-store-get
    ((store controlled-conflict-state-store) key)
  (resilience-kit:state-store-get
   (controlled-conflict-state-store-delegate store)
   key))

(defmethod resilience-kit:state-store-put-if-version
    ((store controlled-conflict-state-store) key value expected-version)
  (if (plusp (controlled-conflict-state-store-remaining-conflicts store))
      (progn
        (decf (controlled-conflict-state-store-remaining-conflicts store))
        (error 'resilience-kit:resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version expected-version))
      (resilience-kit:state-store-put-if-version
       (controlled-conflict-state-store-delegate store)
       key
       value
       expected-version)))

(defun seed-distributed-state
    (store key &key (state :closed) (failure-count 0) opened-at
          (active-probes 0) (half-open-successes 0) (generation 0))
  (resilience-kit:state-store-put-if-version
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
  (it "exports the nerima-lisp package as the primary package name"
    (let ((package (find-package "RESILIENCE-KIT")))
      (expect package :to-be-truthy)
      (when package
        (expect (string= (package-name package)
                         "RESILIENCE-KIT")
                :to-be-truthy)
        (expect (member "CL-RESILIENCE-KIT"
                        (package-nicknames package)
                        :test #'string=)
                :to-be-truthy)
        (expect (find-symbol "MAKE-RETRY-POLICY" package)
                :to-be-truthy))))

  (it "records metrics and isolates observer failures"
    (let* ((metrics (resilience-kit:make-resilience-metrics))
           (event (resilience-kit:make-resilience-event
                    :type :success :operation :read :duration 1.5d0))
           (undated (resilience-kit:make-resilience-event
                      :type :success :operation :read))
           (negative-duration (resilience-kit:make-resilience-event
                               :type :success :operation :read
                               :duration -1d0))
           (overflowing-duration (resilience-kit:make-resilience-event
                                  :type :success :operation :read
                                  :duration (expt 2 20000)))
           (calls 0)
           (observer
             (resilience-kit:make-resilience-observer
              (lambda (ignored)
                (declare (ignore ignored))
                (error "observer failure"))
              (lambda (ignored)
                (declare (ignore ignored))
                (incf calls)))))
      (expect (resilience-kit:record-resilience-event metrics event)
              :to-be-truthy)
      (resilience-kit:record-resilience-event metrics undated)
      (resilience-kit:record-resilience-event metrics negative-duration)
      (resilience-kit:record-resilience-event metrics overflowing-duration)
      (expect (resilience-kit:resilience-metrics-total-events metrics)
              :to-be 4)
      (expect (resilience-kit:resilience-metrics-count
               metrics :success :operation :read)
              :to-be 4)
      (expect (resilience-kit:resilience-metrics-duration
               metrics :success :operation :read)
              :to-be 1.5d0)
      (let ((snapshot (resilience-kit:resilience-metrics-snapshot metrics)))
        (expect (getf snapshot :total-events) :to-be 4)
        (expect (length (getf snapshot :events)) :to-be 1)
        (expect (length (getf snapshot :durations)) :to-be 1))
      (expect (resilience-kit:resilience-observer-handlers observer)
              :to-be-truthy)
      (let ((warnings nil))
        (handler-bind
            ((resilience-kit::resilience-observer-warning
               (lambda (warning)
                 (push warning warnings)
                 (muffle-warning warning))))
          (let ((returned
                  (funcall (resilience-kit:resilience-observer-handler observer)
                           event)))
            (expect returned :to-be-truthy)))
        (expect (length warnings) :to-be 1)
        (expect
         (eq (resilience-kit::resilience-observer-warning-phase
              (first warnings))
             :observer)
         :to-be-truthy))
      (expect calls :to-be 1)
      (expect (resilience-kit:reset-resilience-metrics metrics)
              :to-be-truthy)
      (expect (resilience-kit:resilience-metrics-total-events metrics)
              :to-be 0)))

  (it "uses injected clocks for breaker event timestamps"
    (let* ((fixture (make-test-fixture :start 42))
           (events nil)
           (breaker
             (resilience-kit:make-circuit-breaker
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (resilience-kit:circuit-breaker-call
        breaker (lambda () :ok)
        :operation :read
        :event-handler (lambda (event) (push event events)))
       :to-be
       :ok)
      (expect (length events) :to-be 1)
      (expect
       (approximately-equal-p
        (resilience-kit:resilience-event-timestamp (first events))
        42)
       :to-be-truthy))
    (let* ((fixture (make-test-fixture :start 42))
           (store (resilience-kit:make-memory-state-store))
           (events nil)
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key "service-a"
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+)))
      (expect
       (resilience-kit:distributed-circuit-breaker-call
        breaker (lambda () :ok)
        :operation :read
        :event-handler (lambda (event) (push event events)))
       :to-be
       :ok)
      (expect (length events) :to-be 1)
      (expect
       (approximately-equal-p
        (resilience-kit:resilience-event-timestamp (first events))
        42)
       :to-be-truthy)))

  (it "preserves structured condition data"
    (let ((condition
            (make-condition 'resilience-kit:deadline-exceeded
                            :operation :read
                            :deadline 5d0
                            :observed-at 5d0
                            :stage :wait
                            :attempt 2)))
      (expect (typep condition 'resilience-kit:resilience-error)
              :to-be-truthy)
      (expect (resilience-kit:deadline-exceeded-stage condition)
              :to-be
              :wait)
      (expect (resilience-kit:deadline-exceeded-attempt condition)
              :to-be
              2)
      (expect (stringp (princ-to-string condition)) :to-be-truthy)))

  (it "copies state values and exposes the store protocol error"
    (let* ((store (resilience-kit:make-memory-state-store))
           (value (list :nested (list 1 2)))
           (original (list :nested (list 1 2)))
           (version
             (resilience-kit:state-store-put-if-version
              store "state/a" value nil)))
      (expect version :to-be 1)
      (setf (second (getf value :nested)) 9)
      (multiple-value-bind (loaded loaded-version)
          (resilience-kit:state-store-get store "state/a")
        (expect loaded-version :to-be 1)
        (expect loaded :to-equal original)
        (setf (second (getf loaded :nested)) 8))
      (multiple-value-bind (fresh fresh-version)
          (resilience-kit:state-store-get store "state/a")
        (expect fresh-version :to-be 1)
        (expect fresh :to-equal original))
      (expect (length
               (resilience-kit:state-store-scan-prefix store "state/"))
              :to-be 1)
      (let ((condition
              (expect-condition
               (lambda ()
                 (resilience-kit:state-store-get
                  (make-instance 'resilience-kit:resilience-state-store)
                  "missing"))
               'resilience-kit:resilience-store-error)))
        (expect condition :to-be-truthy)
        (expect (resilience-kit:resilience-store-error-key condition)
                :to-be
                "missing"))))

  (it "enforces fencing lease ownership and supports soft contention"
    (let* ((fixture (make-test-fixture :start 10))
           (store
             (resilience-kit:make-memory-lease-store
              :clock (test-fixture-clock fixture)
              :monotonic-units-per-second
              +test-monotonic-units-per-second+))
           (first-lease
             (resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 10))
           (same-owner
             (resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-a" :ttl 10))
           (soft-contention
             (resilience-kit:acquire-resilience-lease
              store "lease/a" "owner-b" :ttl 10
              :signal-on-unavailable-p nil)))
      (expect first-lease :to-be-truthy)
      (expect (resilience-kit:resilience-lease-fencing-token same-owner)
              :to-be
              (resilience-kit:resilience-lease-fencing-token first-lease))
      (expect soft-contention :to-be nil)
      (let ((condition
              (expect-condition
               (lambda ()
                 (resilience-kit:acquire-resilience-lease
                  store "lease/a" "owner-b" :ttl 10))
               'resilience-kit:resilience-lease-unavailable)))
        (expect condition :to-be-truthy)
        (expect (resilience-kit:resilience-lease-unavailable-retry-after
                 condition)
                :to-be-truthy))
      (expect (resilience-kit:resilience-lease-held-p first-lease)
              :to-be-truthy)
      (expect (resilience-kit:release-resilience-lease first-lease)
              :to-be-truthy)
      (expect (resilience-kit:resilience-lease-held-p first-lease)
              :to-be
              nil))))

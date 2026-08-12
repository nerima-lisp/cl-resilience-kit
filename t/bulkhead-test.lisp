(in-package #:resilience-kit/test)

(describe "bulkheads"
  (it "validates bulkhead capacity and queue configuration"
    (expect
     (lambda () (resilience-kit:make-bulkhead :limit 0))
     :to-throw
     'error)
    (expect
     (lambda () (resilience-kit:make-bulkhead :limit 1.5))
     :to-throw
     'error)
    (expect
     (lambda ()
       (resilience-kit:make-queued-bulkhead :limit 0 :max-queue 0))
     :to-throw
     'error)
    (expect
     (lambda ()
       (resilience-kit:make-queued-bulkhead :limit 1 :max-queue -1))
     :to-throw
     'error)
    (expect
     (lambda ()
       (resilience-kit:make-queued-bulkhead :limit 1 :max-queue 0.5))
     :to-throw
     'error))

  (it "routes queued bulkheads through the generic bulkhead call"
    (let ((bulkhead
            (resilience-kit:make-queued-bulkhead
             :limit 1
             :max-queue 0)))
      (expect (bulkhead-call bulkhead (lambda () :queued))
              :to-be
              :queued)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "preserves multiple values through bulkhead calls"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (multiple-value-bind (first second)
          (bulkhead-call bulkhead (lambda () (values :first :second)))
        (expect first :to-be :first)
        (expect second :to-be :second)))
    (let ((bulkhead
            (resilience-kit:make-queued-bulkhead
             :limit 1
             :max-queue 0)))
      (multiple-value-bind (first second)
          (resilience-kit:queued-bulkhead-call
           bulkhead
           (lambda () (values :first :second)))
        (expect first :to-be :first)
        (expect second :to-be :second))))

  (it "rejects a bulkhead call while its only slot is occupied"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (with-blocking-operation-thread
          (entered release)
          (bulkhead-call
           bulkhead
           (make-blocking-operation entered release))
        (expect (bulkhead-in-flight bulkhead) :to-be 1)
        (let ((rejected (expect-condition
                         (lambda ()
                           (bulkhead-call bulkhead (lambda () :not-run)))
                         'bulkhead-rejected)))
          (expect (typep rejected 'bulkhead-rejected) :to-be-truthy)
          (expect (bulkhead-rejected-in-flight rejected) :to-be 1)))
      (expect (bulkhead-limit bulkhead) :to-be 1)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "releases a bulkhead slot when the operation signals"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (expect
       (lambda ()
         (bulkhead-call bulkhead (lambda () (error "failed"))))
       :to-throw
       'error)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "observes cancellation after return and releases the bulkhead slot"
    (let* ((token (make-cancellation-token))
           (bulkhead (make-bulkhead :limit 1)))
      (expect-condition
       (lambda ()
         (bulkhead-call
          bulkhead
          (lambda ()
            (cancel-cancellation-token token :completed-elsewhere)
            :returned)
          :cancellation-token token))
       'resilience-cancelled)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)
      (expect (bulkhead-call bulkhead (lambda () :available))
              :to-be
              :available)))

  (it "releases a bulkhead slot when admission observation exits nonlocally"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (expect
       (catch :observer-exit
         (bulkhead-call
          bulkhead
          (lambda () :not-reached)
          :event-handler
          (lambda (event)
            (declare (ignore event))
            (throw :observer-exit :escaped))))
       :to-be :escaped)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)
      (expect (bulkhead-call bulkhead (lambda () :available))
              :to-be :available)))

  (it "admits a bounded queued caller and releases both slots"
    (let* ((bulkhead (resilience-kit:make-queued-bulkhead :limit 1 :max-queue 1))
           (waiter nil))
      (with-held-queued-bulkhead (bulkhead entered release)
        (setf waiter
              (make-thread
               (lambda ()
                 (resilience-kit:queued-bulkhead-call
                  bulkhead
                  (lambda () :queued)))))
        (expect-poll
         (lambda ()
           (resilience-kit:queued-bulkhead-queue-size bulkhead))
         (:timeout-ms 500 :interval-ms 1)
         :to-be 1)
        (expect (bulkhead-in-flight bulkhead) :to-be 1)
        (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
                :to-be
                1))
      (expect (join-thread waiter :timeout 5) :to-be :queued)
      (expect (bulkhead-in-flight bulkhead) :to-be 0)
      (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
              :to-be
              0))))

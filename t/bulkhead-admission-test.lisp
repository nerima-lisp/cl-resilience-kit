(in-package #:resilience-kit/test)

(describe "bulkhead admission cancellation boundaries"
  (it "provides a block-oriented non-blocking bulkhead interface"
    (let ((bulkhead (make-bulkhead :limit 1)))
      (expect
       (resilience-kit:with-bulkhead
           (bulkhead :operation :macro)
         :ok)
       :to-be
       :ok)
      (expect (resilience-kit:bulkhead-in-flight bulkhead)
              :to-be
              0)))

  (it "provides a block-oriented queued bulkhead interface"
    (let ((bulkhead (resilience-kit:make-queued-bulkhead
                     :limit 1
                     :max-queue 0)))
      (expect
       (resilience-kit:with-queued-bulkhead
           (bulkhead :operation :queued-macro)
         :ok)
       :to-be
       :ok)
      (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
              :to-be
              0)))

  (it "checks a live cancellation token around admitted work"
    (let* ((token (make-cancellation-token))
           (bulkhead (resilience-kit:make-queued-bulkhead
                      :limit 1
                      :max-queue 0)))
      (expect
       (resilience-kit:queued-bulkhead-call
        bulkhead
        (lambda () :ok)
        :cancellation-token token
        :operation :live-token)
       :to-be
       :ok)
      (expect (resilience-kit:bulkhead-in-flight bulkhead)
              :to-be
              0))))

  (it "rejects a full queued bulkhead with a structured reason"
    (let ((bulkhead (resilience-kit:make-queued-bulkhead
                     :limit 1 :max-queue 0)))
      (with-held-queued-bulkhead (bulkhead entered release)
        (let ((rejected
                (expect-condition
                 (lambda ()
                   (resilience-kit:queued-bulkhead-call
                    bulkhead
                    (lambda () :not-run)))
                 'resilience-kit:resilience-execution-rejected)))
          (expect
           (resilience-kit:resilience-execution-rejected-reason
            rejected)
           :to-be :queue-full)
          (expect
           (resilience-kit:resilience-execution-rejected-queue-size
            rejected)
           :to-be 0))
        (expect (bulkhead-in-flight bulkhead) :to-be 1))
      (expect (bulkhead-in-flight bulkhead) :to-be 0)))

  (it "times out while waiting for a queued bulkhead slot"
    (let ((bulkhead (resilience-kit:make-queued-bulkhead
                     :limit 1 :max-queue 1)))
      (with-held-queued-bulkhead (bulkhead entered release)
        (let ((rejected
                (expect-condition
                 (lambda ()
                   (resilience-kit:queued-bulkhead-call
                    bulkhead
                    (lambda () :not-run)
                    :timeout 0.01))
                 'resilience-kit:resilience-execution-rejected)))
          (expect
           (resilience-kit:resilience-execution-rejected-reason
            rejected)
           :to-be :queue-timeout))
        (expect (bulkhead-in-flight bulkhead) :to-be 1)
        (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
                :to-be 0))))

  (it "honors an ambient deadline while waiting for queued admission"
    (let* ((bulkhead (resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 1))
           (releaser nil))
      (with-held-queued-bulkhead (bulkhead entered release)
        (unwind-protect
             (progn
               (setf releaser
                     (make-thread
                      (lambda ()
                        (expect-poll
                         (lambda ()
                           (resilience-kit:queued-bulkhead-queue-size bulkhead))
                         (:timeout-ms 5000 :interval-ms 0)
                         :to-be 1)
                        (signal-semaphore release)
                        :released)))
               (expect
                (call-with-deadline
                 (lambda ()
                   (resilience-kit:queued-bulkhead-call
                    bulkhead
                    (lambda () :ambient-deadline)))
                 :timeout 1d0)
                :to-be
                :ambient-deadline)
               (expect (join-thread releaser :timeout 5) :to-be :released)
               (expect (bulkhead-in-flight bulkhead) :to-be 0)
               (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
                       :to-be
                       0))
          (when releaser
            (join-thread releaser :timeout 5))))))

  (it "uses the earliest ambient or local queue deadline"
    (let* ((bulkhead (resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 1))
           (rejected nil))
      (with-held-queued-bulkhead (bulkhead entered release)
        (setf rejected
              (expect-condition
               (lambda ()
                 (call-with-deadline
                  (lambda ()
                    (resilience-kit:queued-bulkhead-call
                     bulkhead
                     (lambda () :not-run)
                     :timeout 0d0))
                  :timeout 1d0))
               'resilience-kit:resilience-execution-rejected))
        (expect
         (resilience-kit:resilience-execution-rejected-reason
          rejected)
         :to-be
         :queue-timeout)
        (expect (bulkhead-in-flight bulkhead) :to-be 1)
        (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
                :to-be
                0))))

  (it "cancels a queued bulkhead waiter and removes its queue entry"
    (let* ((bulkhead (resilience-kit:make-queued-bulkhead
                      :limit 1 :max-queue 1))
           (token (make-cancellation-token))
           (waiter-done (make-semaphore :count 0))
           (waiter-condition nil)
           (waiter nil))
      (with-held-queued-bulkhead (bulkhead entered release)
        (unwind-protect
             (progn
               (setf waiter
                     (make-thread
                      (lambda ()
                        (unwind-protect
                             (setf waiter-condition
                                   (expect-condition
                                    (lambda ()
                                      (resilience-kit:queued-bulkhead-call
                                       bulkhead
                                       (lambda () :not-run)
                                       :cancellation-token token
                                       :operation :cancelled-waiter))
                                    'resilience-cancelled))
                          (signal-semaphore waiter-done)))))
               (expect
                (expect-poll
                 (lambda ()
                   (resilience-kit:queued-bulkhead-queue-size bulkhead))
                 (:timeout-ms 5000 :interval-ms 0)
                 :to-be 1))
               (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
                       :to-be
                       1)
               (cancel-cancellation-token token :cancelled)
               (expect (wait-on-semaphore waiter-done :timeout 5)
                       :to-be-truthy)
               (expect (typep waiter-condition 'resilience-cancelled)
                       :to-be-truthy)
               (expect (resilience-cancelled-reason waiter-condition)
                       :to-be
                       :cancelled)
               (expect (resilience-kit:queued-bulkhead-queue-size bulkhead)
                       :to-be
                       0)
               (expect (join-thread waiter :timeout 5) :to-be-truthy)
               (expect (bulkhead-in-flight bulkhead) :to-be 1))
          (when waiter
            (join-thread waiter :timeout 5))))))

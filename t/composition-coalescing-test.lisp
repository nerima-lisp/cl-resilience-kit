(in-package #:resilience-kit/test)

(describe "resilience composition coalescing"
  (it "shares in-flight coalesced requests and cleans up their entries"
    (let* ((coalescer (make-request-coalescer))
           (started (make-semaphore))
           (release (make-semaphore))
           (calls 0)
           (follower-calls 0)
           (owner-result nil)
           (owner-error nil)
           (owner-thread
             (make-thread
              (lambda ()
                (unwind-protect
                    (handler-case
                        (setf owner-result
                              (multiple-value-list
                               (call-with-request-coalescing
                                coalescer
                                (lambda ()
                                  (incf calls)
                                  (signal-semaphore started)
                                  (wait-on-semaphore release)
                                  (values :ok :second))
                                :key "shared"
                                :idempotency-fingerprint "v1")))
                      (condition (condition)
                        (setf owner-error condition)))
                  (values))
                :owner-done))))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore started :timeout 5) :to-be-truthy)
             (expect (request-coalescer-size coalescer) :to-be 1)
             (expect-condition
              (lambda ()
                (call-with-request-coalescing
                 coalescer
                 (lambda () (error "fingerprint-conflict-must-not-run"))
                 :key "shared"
                 :idempotency-fingerprint "v2"
                 :operation :conflict))
              'idempotency-conflict)
             (let ((caught
                     (expect-condition
                      (lambda ()
                        (call-with-request-coalescing
                         coalescer
                         (lambda ()
                           (incf follower-calls)
                           :wrong)
                         :key "shared"
                         :idempotency-fingerprint "v1"
                         :timeout 0d0
                         :operation :coalesced-wait))
                      'resilience-execution-timeout)))
               (expect (resilience-execution-timeout-backend caught)
                       :to-be
                       :coalescer-wait))
             (signal-semaphore release)
             (expect (join-thread owner-thread
                                  :default :not-joined
                                  :timeout 5)
                     :to-be
                     :owner-done)
             (expect owner-error :to-be nil)
             (expect owner-result :to-equal '(:ok :second))
             (expect calls :to-be 1)
             (expect follower-calls :to-be 0)
             (expect (request-coalescer-size coalescer) :to-be 0))
        (signal-semaphore release)
        (join-thread owner-thread :default :not-joined :timeout 5))))

  (it "requires an idempotency key for coalescing"
    (let ((coalescer (make-request-coalescer)))
      (expect-condition
       (lambda ()
         (call-with-request-coalescing
          coalescer
          (lambda () :must-not-run)))
       'resilience-kit:idempotency-key-required)
      (expect (request-coalescer-size coalescer) :to-be 0)))

  (it "rejects a fingerprinted follower for an unfingerprinted in-flight request"
    (let* ((coalescer (make-request-coalescer))
           (started (make-semaphore))
           (release (make-semaphore))
           (owner-thread
             (make-thread
              (lambda ()
                (call-with-request-coalescing
                 coalescer
                 (lambda ()
                   (signal-semaphore started)
                   (wait-on-semaphore release)
                   :owner)
                 :key "unfingerprinted"))
              :name "unfingerprinted-owner")))
      (unwind-protect
           (progn
             (expect (wait-on-semaphore started :timeout 5) :to-be-truthy)
             (expect-condition
              (lambda ()
                (call-with-request-coalescing
                 coalescer (lambda () :must-not-run)
                 :key "unfingerprinted"
                 :idempotency-fingerprint "v1"
                 :operation :fingerprint-conflict))
              'idempotency-conflict))
        (signal-semaphore release)
        (expect (join-thread owner-thread
                             :default :not-joined
                             :timeout 5)
                :to-be
                :owner)
        (expect (request-coalescer-size coalescer) :to-be 0))))

  (it "delivers worker failures to coalesced callers"
    (let ((coalescer (make-request-coalescer)))
      (expect-condition
       (lambda ()
         (call-with-request-coalescing
          coalescer
          (lambda () (error "worker failure"))
          :key "worker-error"
          :operation :worker-error))
       'error)
      (expect (request-coalescer-size coalescer) :to-be 0)))

  (it "preserves executor values and rejects submissions after shutdown"
    (let ((executor (make-resilience-executor :size 1)))
      (unwind-protect
           (progn
             (multiple-value-bind (first second)
                 (resilience-executor-call
                  executor
                  (lambda () (values :first :second))
                  :operation :multiple-values)
               (expect first :to-be :first)
               (expect second :to-be :second))
             (expect
              (resilience-executor-call
               executor
               (lambda () :single)
               :operation :single-value)
              :to-be
              :single)
             (resilience-executor-shutdown
              executor
              :wait t
              :timeout 1d0)
             (let ((caught
                     (expect-condition
                      (lambda ()
                        (resilience-kit:resilience-executor-submit
                         executor
                         (lambda () :not-run)
                         :operation :rejected))
                      'resilience-kit:resilience-execution-rejected)))
               (expect
                (resilience-kit:resilience-execution-rejected-reason caught)
                :to-be
                :executor-rejected)
               (expect
                (resilience-kit:resilience-execution-rejected-queue-size
                 caught)
                :to-be
                0)))
        (unless (resilience-kit:resilience-executor-shutdown-p executor)
          (resilience-executor-shutdown executor :wait t)))))

  (it "removes coalesced entries when executor submission is rejected"
    (let* ((executor (make-resilience-executor :size 1))
           (coalescer (make-request-coalescer)))
      (unwind-protect
           (progn
             (resilience-executor-shutdown executor :wait t)
             (let ((caught
                     (expect-condition
                      (lambda ()
                        (call-with-request-coalescing
                         coalescer
                         (lambda () :must-not-run)
                         :executor executor
                         :key "rejected-submission"
                         :operation :rejected-submission))
                      'resilience-kit:resilience-execution-rejected)))
               (expect
                (resilience-kit:resilience-execution-rejected-reason caught)
                :to-be
                :executor-rejected))
             (expect (request-coalescer-size coalescer) :to-be 0))
        (unless (resilience-kit:resilience-executor-shutdown-p executor)
          (resilience-executor-shutdown executor :wait t)))))

  (it "settles and removes a coalesced entry after a nonlocal exit"
    (let* ((coalescer (make-request-coalescer))
           (tag (gensym "COALESCED-EXIT-"))
           (caught
             (expect-condition
              (lambda ()
                (call-with-request-coalescing
                 coalescer
                 (lambda () (throw tag :escaped))
                 :key "nonlocal-exit"
                 :operation :nonlocal-exit))
              'resilience-error)))
      (expect (typep caught 'resilience-error) :to-be-truthy)
      (expect (request-coalescer-size coalescer) :to-be 0))))

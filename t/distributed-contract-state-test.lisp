(in-package #:resilience-kit/test)

(describe "distributed and asynchronous boundary contracts"
  (it "rejects malformed persisted distributed breaker state"
    (dolist (entry (list
                    (list "not-a-list" :broken)
                    (list "negative-count"
                          (list :state :closed :failure-count -1))
                    (list "bad-opened-at"
                          (list :state :open :opened-at :not-a-time))))
      (let ((store (resilience-kit:make-memory-state-store))
            (key (first entry)))
        (resilience-kit:state-store-put-if-version
         store key (second entry) nil)
        (let ((breaker
                (resilience-kit:make-distributed-circuit-breaker
                 :store store :key key)))
          (expect-condition
           (lambda ()
             (resilience-kit:distributed-circuit-breaker-state breaker))
           'resilience-kit:resilience-store-error)))))

  (it "surfaces distributed state-store initialization failure"
    (let ((store
            (make-instance
             'controlled-conflict-state-store
             :delegate (resilience-kit:make-memory-state-store)
             :remaining-conflicts 65)))
      (let ((breaker
              (resilience-kit:make-distributed-circuit-breaker
               :store store
               :key "initialization-conflicts")))
        (expect-condition
         (lambda ()
           (resilience-kit:distributed-circuit-breaker-call
            breaker (lambda () :unreachable)))
         'resilience-kit:resilience-store-error))))

  (it "rejects calls while all distributed half-open probes are active"
    (let* ((store (resilience-kit:make-memory-state-store))
           (key "half-open-probe-limit")
           (breaker
             (resilience-kit:make-distributed-circuit-breaker
              :store store
              :key key
              :half-open-probe-limit 1)))
      (seed-distributed-state
       store key
       :state :half-open
       :active-probes 1
       :generation 7)
      (let ((rejection
              (expect-condition
               (lambda ()
                 (resilience-kit:distributed-circuit-breaker-call
                  breaker (lambda () :unreachable) :operation :probe))
               'resilience-kit:circuit-open)))
        (expect (resilience-kit:circuit-open-state rejection)
                :to-be
                :half-open)))))

(in-package #:resilience-kit/test)

(describe "leases, lifecycle, and health boundaries"
  (it "uses fencing tokens and rejects stale lease owners"
    (with-test-lease-store (fixture store)
      (let* ((first-lease
               (resilience-kit:acquire-resilience-lease
                store "leader" "owner-a" :ttl 2))
             (unavailable
               (expect-condition
                (lambda ()
                  (resilience-kit:acquire-resilience-lease
                   store "leader" "owner-b" :ttl 2))
                'resilience-kit:resilience-lease-unavailable)))
        (expect (resilience-kit:resilience-lease-held-p first-lease)
                :to-be-truthy)
        (expect (resilience-kit:resilience-lease-fencing-token first-lease)
                :to-be 1)
        (expect (resilience-kit:resilience-lease-unavailable-key unavailable)
                :to-be
                "leader")
        (expect
         (resilience-kit:renew-resilience-lease first-lease :ttl 3)
         :to-be
         first-lease)
        (advance-fixture fixture 3)
        (expect (resilience-kit:resilience-lease-held-p first-lease)
                :to-be nil)
        (let ((second-lease
                (resilience-kit:acquire-resilience-lease
                 store "leader" "owner-b" :ttl 2)))
          (expect
           (> (resilience-kit:resilience-lease-fencing-token second-lease)
              (resilience-kit:resilience-lease-fencing-token first-lease))
           :to-be-truthy)
          (expect-condition
           (lambda ()
             (resilience-kit:renew-resilience-lease first-lease))
           'resilience-kit:resilience-lease-lost)
          (expect (resilience-kit:release-resilience-lease second-lease)
                  :to-be-truthy)
          (expect (resilience-kit:resilience-lease-held-p second-lease)
                  :to-be nil)))))

  (it "releases a lease on nonlocal exit"
    (let ((store (resilience-kit:make-memory-lease-store)))
      (expect
       (catch :lease-exit
         (resilience-kit:with-resilience-lease
             (lease store "temporary" "owner")
           (throw :lease-exit :escaped)))
       :to-be
       :escaped)
      (expect
       (resilience-kit:acquire-resilience-lease
        store "temporary" "next-owner")
       :to-be-truthy)))

  (it "drains active lifecycle entries before stopping"
    (let ((lifecycle (resilience-kit:make-resilience-lifecycle)))
      (expect (resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :running)
      (expect
       (resilience-kit:enter-resilience-lifecycle lifecycle
                                                  :operation :read)
       :to-be-truthy)
      (expect (resilience-kit:resilience-lifecycle-active lifecycle)
              :to-be 1)
      (expect (resilience-kit:begin-resilience-drain lifecycle)
              :to-be
              :draining)
      (expect-condition
       (lambda ()
         (resilience-kit:enter-resilience-lifecycle lifecycle
                                                    :operation :late))
       'resilience-kit:resilience-draining)
      (expect (resilience-kit:leave-resilience-lifecycle lifecycle)
              :to-be 0)
      (expect (resilience-kit:await-resilience-drained lifecycle)
              :to-be-truthy)
      (expect (resilience-kit:stop-resilience-lifecycle lifecycle)
              :to-be-truthy)
      (expect (resilience-kit:resilience-lifecycle-state lifecycle)
              :to-be
              :stopped)))

  (it "reports readiness from registered health checks"
    (let ((registry (resilience-kit:make-health-registry)))
      (resilience-kit:register-health-check
       registry :database (lambda () t))
      (resilience-kit:register-health-check
       registry :cache (lambda () nil))
      (expect (resilience-kit:health-live-p registry) :to-be-truthy)
      (expect (resilience-kit:health-ready-p registry) :to-be nil)
      (let ((report (resilience-kit:health-report registry)))
        (expect (length report) :to-be 2)
        (let ((database-entry
                (find :database report
                      :key (lambda (entry) (getf entry :name))))
              (cache-entry
                (find :cache report
                      :key (lambda (entry) (getf entry :name)))))
          (expect (getf database-entry :status) :to-be :healthy)
          (expect (getf database-entry :value) :to-be-truthy)
          (expect (getf cache-entry :status) :to-be :unhealthy)
          (expect (getf cache-entry :value) :to-be nil)))
      (resilience-kit:unregister-health-check registry :cache)
      (expect (resilience-kit:health-ready-p registry) :to-be-truthy))))

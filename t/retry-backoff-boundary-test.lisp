(in-package #:resilience-kit/test)

(describe "backoff boundary policies"
  (it "caps decorrelated jitter at the configured maximum"
    (let ((policy (make-retry-policy
                   :initial-delay 1d0
                   :max-delay 1d0
                   :jitter :decorrelated)))
      (expect
       (compute-backoff-delay policy 2 :previous-delay 100d0)
       :to-be
       1d0))))

(in-package #:resilience-kit)

;;; Distributed circuit breaker public API

(defun %distributed-circuit-breaker-snapshot (breaker)
  (nth-value 0 (%distributed-circuit-breaker-read breaker)))

(defun distributed-circuit-breaker-state (breaker)
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-state-state
   (%distributed-circuit-breaker-snapshot breaker)))

(defun distributed-circuit-breaker-failure-count (breaker)
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-state-failure-count
   (%distributed-circuit-breaker-snapshot breaker)))

(defun distributed-circuit-breaker-opened-at (breaker)
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-state-opened-at
   (%distributed-circuit-breaker-snapshot breaker)))

(defun distributed-circuit-breaker-active-probes (breaker)
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-state-active-probes
   (%distributed-circuit-breaker-snapshot breaker)))

(defun distributed-circuit-breaker-generation (breaker)
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-state-generation
   (%distributed-circuit-breaker-snapshot breaker)))

(defun distributed-circuit-breaker-reset (breaker)
  "Force the shared breaker to CLOSED and invalidate in-flight generations."
  (check-type breaker distributed-circuit-breaker)
  (%distributed-circuit-breaker-with-lease
   breaker
     (lambda ()
     (let ((store (%distributed-circuit-breaker-store breaker))
           (key (%distributed-circuit-breaker-key breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
             (%distributed-circuit-breaker-read breaker)
           (let ((next
                 (%make-distributed-circuit-breaker-state
                    :state :closed
                    :failure-count 0
                    :opened-at nil
                    :active-probes 0
                    :half-open-successes 0
                    :generation
                    (1+ (%distributed-circuit-breaker-state-generation
                         state)))))
             (handler-case
                 (progn
                   (state-store-put-if-version store key next version)
                   (return-from distributed-circuit-breaker-reset breaker))
               (resilience-store-conflict () nil)))))
       (%distributed-circuit-breaker-store-error
        breaker "Could not reset the distributed circuit-breaker state.")))))

(defmacro with-distributed-circuit-breaker
    ((breaker &rest options) &body body)
  "Evaluate BODY through DISTRIBUTED-CIRCUIT-BREAKER-CALL."
  `(distributed-circuit-breaker-call ,breaker
                                     (lambda () ,@body)
                                     ,@options))

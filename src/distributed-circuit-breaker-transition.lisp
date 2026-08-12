(in-package #:resilience-kit)

;;; Distributed circuit breaker state transitions

(defun %distributed-circuit-breaker-begin (breaker operation)
  (%distributed-circuit-breaker-with-lease
   breaker
   (lambda ()
     (let ((store (distributed-circuit-breaker-store breaker))
           (key (distributed-circuit-breaker-key breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
             (%distributed-circuit-breaker-read breaker)
           (let* ((now (%distributed-circuit-breaker-now breaker))
                  (current-state (getf state :state))
                  (generation (getf state :generation)))
             (case current-state
               (:closed
                (return-from %distributed-circuit-breaker-begin
                  (values t
                          :closed
                          generation
                          nil)))
               (:open
                (let* ((opened-at (getf state :opened-at))
                       (retry-at (and opened-at
                                      (+ opened-at
                                         (distributed-circuit-breaker-reset-timeout
                                          breaker)))))
                  (if (and retry-at (>= now retry-at))
                      (let ((next (copy-list state)))
                        (setf (getf next :state) :half-open
                              (getf next :active-probes) 1
                              (getf next :half-open-successes) 0
                              (getf next :failure-count) 0
                              (getf next :generation) (1+ generation))
                        (handler-case
                            (progn
                              (state-store-put-if-version store key next version)
                              (return-from %distributed-circuit-breaker-begin
                                (values t
                                        :half-open
                                        (getf next :generation)
                                        nil)))
                          (resilience-store-conflict () nil)))
                      (return-from %distributed-circuit-breaker-begin
                        (values nil nil nil
                                (%distributed-circuit-breaker-open-condition
                                 :open retry-at generation operation))))))
               (:half-open
                (if (< (getf state :active-probes)
                       (distributed-circuit-breaker-half-open-probe-limit
                        breaker))
                    (let ((next (copy-list state)))
                      (incf (getf next :active-probes))
                      (handler-case
                          (progn
                            (state-store-put-if-version store key next version)
                            (return-from %distributed-circuit-breaker-begin
                              (values t
                                      :half-open
                                      generation
                                      nil)))
                        (resilience-store-conflict () nil)))
                    (return-from %distributed-circuit-breaker-begin
                      (values nil nil nil
                              (%distributed-circuit-breaker-open-condition
                               :half-open nil generation operation)))))
               (otherwise
                (%distributed-circuit-breaker-store-error
                 breaker "The distributed circuit-breaker state is invalid.")))))))
       (%distributed-circuit-breaker-store-error
        breaker "Could not reserve a distributed circuit-breaker call."))))

(defun %distributed-circuit-breaker-finish
    (breaker token-state token-generation failed-p)
  (%distributed-circuit-breaker-with-lease
   breaker
   (lambda ()
     (let ((store (distributed-circuit-breaker-store breaker))
           (key (distributed-circuit-breaker-key breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
             (%distributed-circuit-breaker-read breaker)
           (when (/= token-generation (getf state :generation))
             (return-from %distributed-circuit-breaker-finish nil))
           (when (not (eq token-state (getf state :state)))
             (return-from %distributed-circuit-breaker-finish nil))
           (let ((next (copy-list state)))
             (case token-state
               (:closed
                (if failed-p
                    (let ((failure-count (1+ (getf state :failure-count))))
                      (if (>= failure-count
                              (distributed-circuit-breaker-failure-threshold
                               breaker))
                          (setf (getf next :state) :open
                                (getf next :opened-at)
                                (%distributed-circuit-breaker-now breaker)
                                (getf next :failure-count) 0
                                (getf next :active-probes) 0
                                (getf next :half-open-successes) 0
                                (getf next :generation)
                                (1+ token-generation))
                          (setf (getf next :failure-count) failure-count)))
                    (setf (getf next :failure-count) 0)))
               (:half-open
                (setf (getf next :active-probes)
                      (max 0 (1- (getf state :active-probes))))
                (if failed-p
                    (setf (getf next :state) :open
                          (getf next :opened-at)
                          (%distributed-circuit-breaker-now breaker)
                          (getf next :failure-count) 0
                          (getf next :active-probes) 0
                          (getf next :half-open-successes) 0
                          (getf next :generation) (1+ token-generation))
                    (let ((successes (1+ (getf state :half-open-successes))))
                      (if (>= successes
                              (distributed-circuit-breaker-success-threshold
                               breaker))
                          (setf (getf next :state) :closed
                                (getf next :opened-at) nil
                                (getf next :failure-count) 0
                                (getf next :active-probes) 0
                                (getf next :half-open-successes) 0
                                (getf next :generation) (1+ token-generation))
                          (setf (getf next :half-open-successes) successes)))))
               (otherwise
                (%distributed-circuit-breaker-store-error
                 breaker "The distributed circuit-breaker token is invalid.")))
             (handler-case
                 (return-from %distributed-circuit-breaker-finish
                   (state-store-put-if-version store key next version))
               (resilience-store-conflict () nil)))))
       (%distributed-circuit-breaker-store-error
        breaker "Could not record the distributed circuit-breaker result.")))))

(defun %distributed-circuit-breaker-finish-classified
    (breaker token-state token-generation classifier value)
  (let ((failed-p nil)
        (classifier-error nil))
    (handler-case
        (setf failed-p
              (and classifier
                   (not (null (funcall classifier value 1)))))
      (error (condition)
        (setf failed-p t
              classifier-error condition)))
    (%distributed-circuit-breaker-finish
     breaker token-state token-generation failed-p)
    (values failed-p classifier-error)))

(in-package #:resilience-kit)

;;; Distributed circuit breaker state transitions

(defun %distributed-circuit-breaker-begin (breaker operation)
  (%distributed-circuit-breaker-with-lease
   breaker
   (lambda ()
     (let ((store (%distributed-circuit-breaker-store breaker))
           (key (%distributed-circuit-breaker-key breaker))
           (reset-timeout
             (%distributed-circuit-breaker-reset-timeout breaker))
           (half-open-probe-limit
             (%distributed-circuit-breaker-half-open-probe-limit breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
           (%distributed-circuit-breaker-read breaker)
           (let* ((now (%distributed-circuit-breaker-now breaker))
                  (current-state
                    (%distributed-circuit-breaker-state-state state))
                  (generation
                    (%distributed-circuit-breaker-state-generation state)))
             (case current-state
               (:closed
                (return-from %distributed-circuit-breaker-begin
                  (values t
                          :closed
                          generation
                          nil)))
               (:open
                (let* ((opened-at
                         (%distributed-circuit-breaker-state-opened-at state))
                       (retry-at (and opened-at
                                      (+ opened-at reset-timeout))))
                  (if (and retry-at (>= now retry-at))
                      (let ((next
                              (%make-distributed-circuit-breaker-state
                               :state :half-open
                               :failure-count 0
                               :opened-at opened-at
                               :active-probes 1
                               :half-open-successes 0
                               :generation (1+ generation))))
                        (handler-case
                            (progn
                              (state-store-put-if-version store key next version)
                              (return-from %distributed-circuit-breaker-begin
                                (values t
                                        :half-open
                                        (%distributed-circuit-breaker-state-generation
                                         next)
                                        nil)))
                          (resilience-store-conflict () nil)))
                      (return-from %distributed-circuit-breaker-begin
                        (values nil nil nil
                                (%distributed-circuit-breaker-open-condition
                                 :open retry-at generation operation))))))
               (:half-open
                (let ((active-probes
                        (%distributed-circuit-breaker-state-active-probes state))
                      (failure-count
                        (%distributed-circuit-breaker-state-failure-count state))
                      (opened-at
                        (%distributed-circuit-breaker-state-opened-at state))
                      (half-open-successes
                        (%distributed-circuit-breaker-state-half-open-successes
                         state)))
                  (if (< active-probes half-open-probe-limit)
                      (let ((next
                              (%make-distributed-circuit-breaker-state
                               :state :half-open
                               :failure-count failure-count
                               :opened-at opened-at
                               :active-probes (1+ active-probes)
                               :half-open-successes half-open-successes
                               :generation generation)))
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
                                 :half-open nil generation operation))))))
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
     (let ((store (%distributed-circuit-breaker-store breaker))
           (key (%distributed-circuit-breaker-key breaker))
           (failure-threshold
             (%distributed-circuit-breaker-failure-threshold breaker))
           (success-threshold
             (%distributed-circuit-breaker-success-threshold breaker)))
       (loop repeat 64 do
         (multiple-value-bind (state version)
           (%distributed-circuit-breaker-read breaker)
           (let ((now (%distributed-circuit-breaker-now breaker))
                 (generation
                   (%distributed-circuit-breaker-state-generation state))
                 (current-state
                   (%distributed-circuit-breaker-state-state state))
                 (failure-count
                   (%distributed-circuit-breaker-state-failure-count state))
                 (opened-at
                   (%distributed-circuit-breaker-state-opened-at state))
                 (active-probes
                   (%distributed-circuit-breaker-state-active-probes state))
                 (half-open-successes
                   (%distributed-circuit-breaker-state-half-open-successes
                    state)))
             (when (/= token-generation generation)
               (return-from %distributed-circuit-breaker-finish nil))
             (when (not (eq token-state current-state))
               (return-from %distributed-circuit-breaker-finish nil))
             (let ((next
                     (case token-state
                       (:closed
                        (if failed-p
                            (let ((next-failure-count (1+ failure-count)))
                              (if (>= next-failure-count failure-threshold)
                                  (%make-distributed-circuit-breaker-state
                                   :state :open
                                   :failure-count 0
                                   :opened-at now
                                   :active-probes 0
                                   :half-open-successes 0
                                   :generation (1+ token-generation))
                                  (%make-distributed-circuit-breaker-state
                                   :state :closed
                                   :failure-count next-failure-count
                                   :opened-at opened-at
                                   :active-probes active-probes
                                   :half-open-successes half-open-successes
                                   :generation generation)))
                            (%make-distributed-circuit-breaker-state
                             :state :closed
                             :failure-count 0
                             :opened-at opened-at
                             :active-probes active-probes
                             :half-open-successes half-open-successes
                             :generation generation)))
                       (:half-open
                        (let ((next-active-probes (max 0 (1- active-probes))))
                          (if failed-p
                              (%make-distributed-circuit-breaker-state
                               :state :open
                               :failure-count 0
                               :opened-at now
                               :active-probes 0
                               :half-open-successes 0
                               :generation (1+ token-generation))
                              (let ((successes (1+ half-open-successes)))
                                (if (>= successes success-threshold)
                                    (%make-distributed-circuit-breaker-state
                                     :state :closed
                                     :failure-count 0
                                     :opened-at nil
                                     :active-probes 0
                                     :half-open-successes 0
                                     :generation (1+ token-generation))
                                    (%make-distributed-circuit-breaker-state
                                     :state :half-open
                                     :failure-count failure-count
                                     :opened-at opened-at
                                     :active-probes next-active-probes
                                     :half-open-successes successes
                                     :generation generation))))))
                       (otherwise
                        (%distributed-circuit-breaker-store-error
                         breaker "The distributed circuit-breaker token is invalid.")))))
               (handler-case
                   (return-from %distributed-circuit-breaker-finish
                     (state-store-put-if-version store key next version))
                 (resilience-store-conflict () nil))))))
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

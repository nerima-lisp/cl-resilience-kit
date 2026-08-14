(in-package #:resilience-kit)

(defun %circuit-open-condition
    (state retry-at generation operation)
  (make-condition
   'circuit-open
   :message (if (eq state :half-open)
                "The circuit breaker has no available half-open probe."
                "The circuit breaker is open.")
   :operation operation
   :state state
   :retry-at retry-at
   :generation generation))

(defun %circuit-breaker-begin (breaker operation)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    (let* ((now (%circuit-breaker-now breaker))
           (current-state (%circuit-breaker-state breaker))
           (generation (%circuit-breaker-generation breaker)))
      (case current-state
        (:closed
         (values t
                 :closed
                 generation
                 nil))
        (:open
         (let* ((opened-at (%circuit-breaker-opened-at breaker))
                (reset-timeout (%circuit-breaker-reset-timeout breaker))
                (retry-at (and opened-at
                               (+ opened-at reset-timeout))))
           (if (and retry-at (>= now retry-at))
               (let ((next-generation (1+ generation)))
                 (setf (%circuit-breaker-state breaker) :half-open
                       (%circuit-breaker-generation breaker) next-generation
                       (%circuit-breaker-active-probes breaker) 1
                       (%circuit-breaker-half-open-successes breaker) 0)
                 (values t
                         :half-open
                         next-generation
                         nil))
               (values nil nil nil
                       (%circuit-open-condition
                        :open retry-at generation
                        operation)))))
        (:half-open
         (let ((active-probes (%circuit-breaker-active-probes breaker))
               (probe-limit (%circuit-breaker-half-open-probe-limit breaker)))
           (if (< active-probes probe-limit)
             (progn
               (incf (%circuit-breaker-active-probes breaker))
               (values t
                       :half-open
                       generation
                       nil))
             (values nil nil nil
                     (%circuit-open-condition
                      :half-open nil generation
                      operation)))))
        (otherwise
         (error "Unknown circuit breaker state ~S."
                current-state))))))

(defun %circuit-breaker-open! (breaker now)
  (setf (%circuit-breaker-state breaker) :open
        (%circuit-breaker-opened-at breaker) now
        (%circuit-breaker-failure-count breaker) 0
        (%circuit-breaker-active-probes breaker) 0
        (%circuit-breaker-half-open-successes breaker) 0
        (%circuit-breaker-generation breaker)
        (1+ (%circuit-breaker-generation breaker))))

(defun %circuit-breaker-finish
    (breaker token-state token-generation failed-p)
  (cl-concurrent-kit:with-lock-held ((%circuit-breaker-lock breaker))
    ;; A completion from an obsolete generation cannot overwrite a newer
    ;; reset, close, or reopen transition.
    (let ((current-generation (%circuit-breaker-generation breaker)))
      (when (= token-generation current-generation)
        (case token-state
          (:closed
           (when (eq (%circuit-breaker-state breaker) :closed)
             (if failed-p
                 (let* ((next-failure-count
                          (1+ (%circuit-breaker-failure-count breaker)))
                        (failure-threshold
                          (%circuit-breaker-failure-threshold breaker)))
                   (if (>= next-failure-count failure-threshold)
                       (%circuit-breaker-open!
                        breaker (%circuit-breaker-now breaker))
                       (setf (%circuit-breaker-failure-count breaker)
                             next-failure-count)))
                 (setf (%circuit-breaker-failure-count breaker) 0))))
          (:half-open
           (when (eq (%circuit-breaker-state breaker) :half-open)
             (let ((remaining-probes
                     (max 0 (1- (%circuit-breaker-active-probes breaker)))))
               (setf (%circuit-breaker-active-probes breaker)
                     remaining-probes))
             (if failed-p
                 (%circuit-breaker-open!
                  breaker (%circuit-breaker-now breaker))
                 (let* ((next-success-count
                          (1+ (%circuit-breaker-half-open-successes breaker)))
                        (success-threshold
                          (%circuit-breaker-success-threshold breaker)))
                   (if (>= next-success-count success-threshold)
                       (setf (%circuit-breaker-state breaker) :closed
                             (%circuit-breaker-opened-at breaker) nil
                             (%circuit-breaker-failure-count breaker) 0
                             (%circuit-breaker-active-probes breaker) 0
                             (%circuit-breaker-half-open-successes breaker) 0
                             (%circuit-breaker-generation breaker)
                             (1+ current-generation))
                       (setf (%circuit-breaker-half-open-successes breaker)
                             next-success-count)))))))))))

(defun %circuit-breaker-finish-classified
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
    (%circuit-breaker-finish
     breaker token-state token-generation failed-p)
    (values failed-p classifier-error)))

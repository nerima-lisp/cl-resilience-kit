(in-package #:cl-resilience-kit)

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
    (let ((now (%circuit-breaker-now breaker)))
      (case (%circuit-breaker-state breaker)
        (:closed
         (values t
                 :closed
                 (%circuit-breaker-generation breaker)
                 nil))
        (:open
         (let* ((opened-at (%circuit-breaker-opened-at breaker))
                (retry-at (and opened-at
                               (+ opened-at
                                  (circuit-breaker-reset-timeout breaker)))))
           (if (and retry-at (>= now retry-at))
               (progn
                 (setf (%circuit-breaker-state breaker) :half-open
                       (%circuit-breaker-generation breaker)
                       (1+ (%circuit-breaker-generation breaker))
                 (%circuit-breaker-active-probes breaker) 1
                       (%circuit-breaker-half-open-successes breaker) 0)
                 (values t
                         :half-open
                         (%circuit-breaker-generation breaker)
                         nil))
               (values nil nil nil
                       (%circuit-open-condition
                        :open retry-at (%circuit-breaker-generation breaker)
                        operation)))))
        (:half-open
         (if (< (%circuit-breaker-active-probes breaker)
                (circuit-breaker-half-open-probe-limit breaker))
             (progn
               (incf (%circuit-breaker-active-probes breaker))
               (values t
                       :half-open
                       (%circuit-breaker-generation breaker)
                       nil))
             (values nil nil nil
                     (%circuit-open-condition
                      :half-open nil (%circuit-breaker-generation breaker)
                      operation))))
        (otherwise
         (error "Unknown circuit breaker state ~S."
                (%circuit-breaker-state breaker)))))))

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
    (when (= token-generation (%circuit-breaker-generation breaker))
      (case token-state
          (:closed
           (when (eq (%circuit-breaker-state breaker) :closed)
             (if failed-p
                 (progn
                   (incf (%circuit-breaker-failure-count breaker))
                   (when (>= (%circuit-breaker-failure-count breaker)
                             (circuit-breaker-failure-threshold breaker))
                     (%circuit-breaker-open!
                      breaker (%circuit-breaker-now breaker))))
                 (setf (%circuit-breaker-failure-count breaker) 0))))
          (:half-open
           (when (eq (%circuit-breaker-state breaker) :half-open)
             (setf (%circuit-breaker-active-probes breaker)
                   (max 0 (1- (%circuit-breaker-active-probes breaker))))
             (if failed-p
                 (%circuit-breaker-open!
                  breaker (%circuit-breaker-now breaker))
                 (progn
                   (incf (%circuit-breaker-half-open-successes breaker))
                   (when (>= (%circuit-breaker-half-open-successes breaker)
                             (circuit-breaker-success-threshold breaker))
                     (setf (%circuit-breaker-state breaker) :closed
                           (%circuit-breaker-opened-at breaker) nil
                           (%circuit-breaker-failure-count breaker) 0
                           (%circuit-breaker-active-probes breaker) 0
                           (%circuit-breaker-half-open-successes breaker) 0
                           (%circuit-breaker-generation breaker)
                           (1+ (%circuit-breaker-generation breaker))))))))))))

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

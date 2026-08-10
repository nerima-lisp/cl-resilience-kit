(in-package #:cl-resilience-kit)

;;; Distributed circuit breaker state access

(defun %distributed-circuit-breaker-now (breaker)
  (%monotonic-seconds
   (distributed-circuit-breaker-clock breaker)
   (distributed-circuit-breaker-monotonic-units-per-second breaker)))

(defun %distributed-circuit-breaker-default-state ()
  (list :state :closed
        :failure-count 0
        :opened-at nil
        :active-probes 0
        :half-open-successes 0
        :generation 0))

(defun %distributed-circuit-breaker-store-error (breaker message &optional cause)
  (error 'resilience-store-error
         :key (distributed-circuit-breaker-key breaker)
         :cause cause
         :message message))

(defun %distributed-circuit-breaker-normalize-state (breaker value)
  (unless (%proper-list-p value)
    (%distributed-circuit-breaker-store-error
     breaker "The distributed circuit-breaker state is not a property list."
     value))
  (let ((state (getf value :state))
        (failure-count (getf value :failure-count))
        (opened-at (getf value :opened-at))
        (active-probes (getf value :active-probes))
        (half-open-successes (getf value :half-open-successes))
        (generation (getf value :generation)))
    (unless (member state '(:closed :open :half-open))
      (%distributed-circuit-breaker-store-error
       breaker "The distributed circuit-breaker state has an invalid state."
       value))
    (dolist (entry (list (cons :failure-count failure-count)
                         (cons :active-probes active-probes)
                         (cons :half-open-successes half-open-successes)
                         (cons :generation generation)))
      (unless (and (integerp (cdr entry)) (>= (cdr entry) 0))
        (%distributed-circuit-breaker-store-error
         breaker
         (format nil "The distributed circuit-breaker field ~S is invalid."
                 (car entry))
         value)))
    (when (and opened-at (not (%finite-real-p opened-at)))
      (%distributed-circuit-breaker-store-error
       breaker "The distributed circuit-breaker opened-at value is invalid."
       value))
    (list :state state
          :failure-count failure-count
          :opened-at opened-at
          :active-probes active-probes
          :half-open-successes half-open-successes
          :generation generation)))

(defun %distributed-circuit-breaker-read (breaker)
  (let ((store (distributed-circuit-breaker-store breaker))
        (key (distributed-circuit-breaker-key breaker)))
    (loop repeat 64 do
      (multiple-value-bind (value version)
          (state-store-get store key)
        (if version
            (return-from %distributed-circuit-breaker-read
              (values (%distributed-circuit-breaker-normalize-state
                       breaker value)
                      version))
            (handler-case
                (let* ((initial (%distributed-circuit-breaker-default-state))
                       (new-version
                         (state-store-put-if-version
                          store key initial nil)))
                  (return-from %distributed-circuit-breaker-read
                    (values initial new-version)))
              (resilience-store-conflict () nil)))))
    (%distributed-circuit-breaker-store-error
     breaker "Could not initialize or read the distributed circuit-breaker state.")))

(defun %distributed-circuit-breaker-with-lease (breaker thunk)
  (let ((lease-store (distributed-circuit-breaker-lease-store breaker)))
    (if lease-store
        (let ((lease
                (acquire-resilience-lease
                 lease-store
                 (distributed-circuit-breaker-key breaker)
                 (distributed-circuit-breaker-lease-owner breaker)
                 :ttl (distributed-circuit-breaker-lease-ttl breaker))))
          (unwind-protect
               (funcall thunk)
            (when lease
              (release-resilience-lease lease :ignore-lost-p t))))
        (funcall thunk))))

(defun %distributed-circuit-breaker-open-condition
    (state retry-at generation operation)
  (make-condition
   'circuit-open
   :message (if (eq state :half-open)
                "The distributed circuit breaker has no available half-open probe."
                "The distributed circuit breaker is open.")
   :operation operation
   :state state
   :retry-at retry-at
   :generation generation))

(in-package #:resilience-kit)

;;; Distributed circuit breaker state access

(defun %distributed-circuit-breaker-now (breaker)
  (%monotonic-seconds
   (%distributed-circuit-breaker-clock breaker)
   (%distributed-circuit-breaker-monotonic-units-per-second breaker)))

(defun %make-distributed-circuit-breaker-state
    (&key state failure-count opened-at active-probes
       half-open-successes generation)
  (list state
        failure-count
        opened-at
        active-probes
        half-open-successes
        generation))

(defun %distributed-circuit-breaker-state-state (value)
  (car value))

(defun %distributed-circuit-breaker-state-failure-count (value)
  (cadr value))

(defun %distributed-circuit-breaker-state-opened-at (value)
  (caddr value))

(defun %distributed-circuit-breaker-state-active-probes (value)
  (cadddr value))

(defun %distributed-circuit-breaker-state-half-open-successes (value)
  (car (cddddr value)))

(defun %distributed-circuit-breaker-state-generation (value)
  (cadr (cddddr value)))

(defun %distributed-circuit-breaker-default-state ()
  (%make-distributed-circuit-breaker-state
   :state :closed
   :failure-count 0
   :opened-at nil
   :active-probes 0
   :half-open-successes 0
   :generation 0))

(defun %distributed-circuit-breaker-store-error (breaker message &optional cause)
  (error 'resilience-store-error
         :key (%distributed-circuit-breaker-key breaker)
         :cause cause
         :message message))

(defun %distributed-circuit-breaker-normalize-state (breaker value)
  (multiple-value-bind (state failure-count opened-at active-probes
                        half-open-successes generation)
      (cond
        ((and (%proper-list-p value)
              (keywordp (car value)))
         (values (getf value :state)
                 (getf value :failure-count)
                 (getf value :opened-at)
                 (getf value :active-probes)
                 (getf value :half-open-successes)
                 (getf value :generation)))
        ((and (%proper-list-p value)
              (eql (list-length value) 6))
         (values (%distributed-circuit-breaker-state-state value)
                 (%distributed-circuit-breaker-state-failure-count value)
                 (%distributed-circuit-breaker-state-opened-at value)
                 (%distributed-circuit-breaker-state-active-probes value)
                 (%distributed-circuit-breaker-state-half-open-successes value)
                 (%distributed-circuit-breaker-state-generation value)))
        (t
         (%distributed-circuit-breaker-store-error
          breaker
          "The distributed circuit-breaker state is not a valid encoded state."
          value)))
    (unless (member state '(:closed :open :half-open))
      (%distributed-circuit-breaker-store-error
       breaker "The distributed circuit-breaker state has an invalid state."
       value))
    (flet ((ensure-non-negative-integer (field field-value)
             (unless (and (integerp field-value) (>= field-value 0))
               (%distributed-circuit-breaker-store-error
                breaker
                (format nil "The distributed circuit-breaker field ~S is invalid."
                        field)
                value))))
      (ensure-non-negative-integer :failure-count failure-count)
      (ensure-non-negative-integer :active-probes active-probes)
      (ensure-non-negative-integer :half-open-successes half-open-successes)
      (ensure-non-negative-integer :generation generation))
    (when (and opened-at (not (%finite-real-p opened-at)))
      (%distributed-circuit-breaker-store-error
       breaker "The distributed circuit-breaker opened-at value is invalid."
       value))
    (%make-distributed-circuit-breaker-state
     :state state
     :failure-count failure-count
     :opened-at opened-at
     :active-probes active-probes
     :half-open-successes half-open-successes
     :generation generation)))

(defun %distributed-circuit-breaker-read (breaker)
  (let ((store (%distributed-circuit-breaker-store breaker))
        (key (%distributed-circuit-breaker-key breaker)))
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
  (let ((lease-store (%distributed-circuit-breaker-lease-store breaker)))
    (if lease-store
        (let* ((key (%distributed-circuit-breaker-key breaker))
               (lease-owner (%distributed-circuit-breaker-lease-owner breaker))
               (lease-ttl (%distributed-circuit-breaker-lease-ttl breaker))
               (lease
                 (acquire-resilience-lease
                  lease-store
                  key
                  lease-owner
                  :ttl lease-ttl)))
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

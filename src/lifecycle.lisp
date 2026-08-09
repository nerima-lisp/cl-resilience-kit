(in-package #:cl-resilience-kit)

;;; Lifecycle and health boundaries

(defclass resilience-lifecycle ()
  ((%state
    :initform :running
    :accessor %resilience-lifecycle-state)
   (%active
    :initform 0
    :accessor %resilience-lifecycle-active)
   (%lock
    :initarg :lock
    :reader %resilience-lifecycle-lock)
   (%condition-variable
    :initarg :condition-variable
    :reader %resilience-lifecycle-condition-variable)))

(defun make-resilience-lifecycle (&key name)
  (make-instance
   'resilience-lifecycle
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.lifecycle"))
   :condition-variable
   (cl-concurrent-kit:make-condition-variable
    :name (or name "cl-resilience-kit.lifecycle.condition"))))

(defun resilience-lifecycle-state (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (%resilience-lifecycle-state lifecycle)))

(defun resilience-lifecycle-active (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (%resilience-lifecycle-active lifecycle)))

(defun resilience-lifecycle-accepting-p (lifecycle)
  (eq (resilience-lifecycle-state lifecycle) :running))

(defun begin-resilience-drain (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (when (eq (%resilience-lifecycle-state lifecycle) :running)
      (setf (%resilience-lifecycle-state lifecycle) :draining)
      (cl-concurrent-kit:condition-broadcast
       (%resilience-lifecycle-condition-variable lifecycle)))
    (%resilience-lifecycle-state lifecycle)))

(defun enter-resilience-lifecycle (lifecycle &key operation)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (unless (eq (%resilience-lifecycle-state lifecycle) :running)
      (error 'resilience-draining
             :operation operation
             :state (%resilience-lifecycle-state lifecycle)
             :message "The resilience lifecycle is draining or stopped."))
    (incf (%resilience-lifecycle-active lifecycle))
    t))

(defun leave-resilience-lifecycle (lifecycle)
  (check-type lifecycle resilience-lifecycle)
  (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
    (when (plusp (%resilience-lifecycle-active lifecycle))
      (decf (%resilience-lifecycle-active lifecycle)))
    (when (zerop (%resilience-lifecycle-active lifecycle))
      (cl-concurrent-kit:condition-broadcast
       (%resilience-lifecycle-condition-variable lifecycle)))
    (%resilience-lifecycle-active lifecycle)))

(defun await-resilience-drained (lifecycle &key timeout)
  "Wait until no operation remains active; return NIL on TIMEOUT."
  (check-type lifecycle resilience-lifecycle)
  (when timeout
    (%ensure-non-negative-real timeout "TIMEOUT"))
  (let ((deadline (and timeout (+ (%now) (float timeout 1d0)))))
    (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
      (loop while (plusp (%resilience-lifecycle-active lifecycle)) do
        (let ((remaining (and deadline (max 0d0 (- deadline (%now))))))
          (when (and deadline (zerop remaining))
            (return-from await-resilience-drained nil))
          (unless (cl-concurrent-kit:condition-wait
                   (%resilience-lifecycle-condition-variable lifecycle)
                   (%resilience-lifecycle-lock lifecycle)
                   :timeout remaining)
            (when (and deadline
                       (plusp (%resilience-lifecycle-active lifecycle)))
              (return-from await-resilience-drained nil)))))
      t)))

(defun stop-resilience-lifecycle (lifecycle &key timeout)
  (check-type lifecycle resilience-lifecycle)
  (begin-resilience-drain lifecycle)
  (when (await-resilience-drained lifecycle :timeout timeout)
    (cl-concurrent-kit:with-lock-held ((%resilience-lifecycle-lock lifecycle))
      (setf (%resilience-lifecycle-state lifecycle) :stopped)
      (cl-concurrent-kit:condition-broadcast
       (%resilience-lifecycle-condition-variable lifecycle)))
    t))

(defmacro with-resilience-lifecycle ((lifecycle &key operation) &body body)
  `(progn
     (enter-resilience-lifecycle ,lifecycle :operation ,operation)
     (unwind-protect
          (progn ,@body)
       (leave-resilience-lifecycle ,lifecycle))))

(defclass health-registry ()
  ((%checks
    :initform (make-hash-table :test #'equal)
    :reader %health-registry-checks)
   (%lock
    :initarg :lock
    :reader %health-registry-lock)))

(defun make-health-registry (&key name)
  (make-instance
   'health-registry
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.health"))))

(defun register-health-check (registry name checker)
  (check-type registry health-registry)
  (check-type checker function)
  (cl-concurrent-kit:with-lock-held ((%health-registry-lock registry))
    (setf (gethash name (%health-registry-checks registry)) checker))
  name)

(defun unregister-health-check (registry name)
  (check-type registry health-registry)
  (cl-concurrent-kit:with-lock-held ((%health-registry-lock registry))
    (remhash name (%health-registry-checks registry)))
  registry)

(defun health-report (registry)
  (check-type registry health-registry)
  (let ((checks
          (cl-concurrent-kit:with-lock-held ((%health-registry-lock registry))
            (loop for name being the hash-keys of (%health-registry-checks registry)
                  using (hash-value checker)
                  collect (cons name checker)))))
    (mapcar
     (lambda (entry)
       (handler-case
           (let ((value (funcall (cdr entry))))
             (list :name (car entry)
                   :status (if value :healthy :unhealthy)
                   :value value))
         (error (condition)
           (list :name (car entry)
                 :status :unhealthy
                 :condition condition))))
     checks)))

(defun health-ready-p (registry)
  (let ((report (health-report registry)))
    (and report
         (every (lambda (entry) (eq (getf entry :status) :healthy)) report))))

(defun health-live-p (registry)
  (declare (ignore registry))
  t)

(in-package #:resilience-kit)

(defun %seconds->duration (seconds)
  (let* ((total-nanos
           (round (* (coerce seconds 'double-float) 1000000000d0)))
         (whole-seconds (floor total-nanos 1000000000))
         (nanos (mod total-nanos 1000000000)))
    (duration-of-seconds whole-seconds nanos)))

(defclass resilience-executor ()
  ((implementation
    :initarg :implementation
    :reader resilience-executor-implementation
    :reader %resilience-executor-implementation)))

(defun make-resilience-executor
    (&key (size 4) name queue-capacity)
  "Create an executor with resilience-specific rejection and value handling."
  (check-type size (integer 1 *))
  (when queue-capacity
    (check-type queue-capacity (integer 1 *)))
  (make-instance 'resilience-executor
                 :implementation
                 (make-executor
                  :size size
                  :name (or name "cl-resilience-kit")
                  :queue-capacity queue-capacity)))

(defun resilience-executor-queue-depth (executor)
  (check-type executor resilience-executor)
  (executor-queue-depth
   (%resilience-executor-implementation executor)))

(defun resilience-executor-queue-capacity (executor)
  (check-type executor resilience-executor)
  (executor-queue-capacity
   (%resilience-executor-implementation executor)))

(defun resilience-executor-high-water-mark (executor)
  (check-type executor resilience-executor)
  (executor-high-water-mark
   (%resilience-executor-implementation executor)))

(defun resilience-executor-shutdown-p (executor)
  (check-type executor resilience-executor)
  (executor-shutdown-p
   (%resilience-executor-implementation executor)))

(defun resilience-executor-terminated-p (executor)
  (check-type executor resilience-executor)
  (executor-terminated-p
   (%resilience-executor-implementation executor)))

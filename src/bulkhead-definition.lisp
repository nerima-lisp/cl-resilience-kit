(in-package #:resilience-kit)

(defclass bulkhead ()
  ((limit
    :initarg :limit
    :reader bulkhead-limit
    :reader %bulkhead-limit)
   (%in-flight
    :initform 0
    :accessor %bulkhead-in-flight)
   (%lock
    :initarg :lock
    :reader %bulkhead-lock)))

(defun make-bulkhead (&key (limit 1))
  "Create a non-blocking, caller-thread concurrency limiter.

When every slot is occupied, BULKHEAD-CALL signals BULKHEAD-REJECTED.  The
implementation owns one ordinary lock and starts no worker, timer, or hidden
queue."
  (unless (and (integerp limit) (plusp limit))
    (error "LIMIT must be a positive integer, got ~S." limit))
  (make-instance 'bulkhead
                 :limit limit
                 :lock (make-lock
                        :name "cl-resilience-kit.bulkhead")))

(defun bulkhead-in-flight (bulkhead)
  (check-type bulkhead bulkhead)
  (with-lock-held ((%bulkhead-lock bulkhead))
    (%bulkhead-in-flight bulkhead)))

;;; A bounded waiting bulkhead.
;;;
;;; BULKHEAD remains deliberately non-blocking.  This subtype is explicit so
;;; callers can choose whether overload is rejected immediately or queued,
;;; and so queueing never appears accidentally in an existing deployment.
(defclass queued-bulkhead (bulkhead)
  ((max-queue
    :initarg :max-queue
    :reader queued-bulkhead-max-queue
    :reader %queued-bulkhead-max-queue)
   (%waiting
    :initform 0
    :accessor %queued-bulkhead-waiting)
   (%condition-variable
    :initarg :condition-variable
    :reader %queued-bulkhead-condition-variable)))

(defun make-queued-bulkhead (&key (limit 1) (max-queue 0))
  "Create a bulkhead that waits for a slot with a bounded admission queue.

MAX-QUEUE is the maximum number of callers waiting for a slot.  A value of
zero preserves immediate rejection when all slots are occupied."
  (unless (and (integerp limit) (plusp limit))
    (error "LIMIT must be a positive integer, got ~S." limit))
  (unless (and (integerp max-queue) (>= max-queue 0))
    (error "MAX-QUEUE must be a non-negative integer, got ~S." max-queue))
  (make-instance
   'queued-bulkhead
   :limit limit
   :max-queue max-queue
   :lock (make-lock
          :name "cl-resilience-kit.queued-bulkhead")
   :condition-variable
   (make-condition-variable
    :name "cl-resilience-kit.queued-bulkhead")))

(defun queued-bulkhead-queue-size (bulkhead)
  "Return the number of callers currently waiting for admission."
  (check-type bulkhead queued-bulkhead)
  (with-lock-held ((%bulkhead-lock bulkhead))
    (%queued-bulkhead-waiting bulkhead)))

(defun %queued-bulkhead-rejection
    (operation reason queue-size)
  (make-condition
   'resilience-execution-rejected
   :message "The queued bulkhead could not admit the operation."
   :operation operation
   :reason reason
   :queue-size queue-size))

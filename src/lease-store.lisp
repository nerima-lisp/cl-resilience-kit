(in-package #:cl-resilience-kit)

(defclass resilience-lease-store () ())

(defgeneric acquire-resilience-lease
    (store key owner &key ttl signal-on-unavailable-p))
(defgeneric renew-resilience-lease (lease &key ttl))
(defgeneric release-resilience-lease (lease &key ignore-lost-p))
(defgeneric resilience-lease-held-p (lease))

(defclass resilience-lease ()
  ((store
    :initarg :store
    :reader resilience-lease-store)
   (key
    :initarg :key
    :reader resilience-lease-key)
   (owner
    :initarg :owner
    :reader resilience-lease-owner)
   (fencing-token
    :initarg :fencing-token
    :reader resilience-lease-fencing-token)
   (expires-at
    :initarg :expires-at
    :accessor resilience-lease-expires-at)
   (ttl
    :initarg :ttl
    :accessor resilience-lease-ttl)))

(defstruct (%lease-record
            (:constructor %make-lease-record
                (owner fencing-token expires-at)))
  owner
  fencing-token
  expires-at)

(defclass memory-lease-store (resilience-lease-store)
  ((%leases
    :initform (make-hash-table :test #'equal)
    :reader %memory-lease-store-leases)
   (%next-fencing-token
    :initform (make-hash-table :test #'equal)
    :reader %memory-lease-store-next-fencing-token)
   (%lock
    :initarg :lock
    :reader %memory-lease-store-lock)
   (clock
    :initarg :clock
    :reader memory-lease-store-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader memory-lease-store-monotonic-units-per-second)))

(defun make-memory-lease-store
    (&key clock monotonic-units-per-second name)
  (make-instance
   'memory-lease-store
   :clock (%active-clock clock)
   :monotonic-units-per-second
   (%active-monotonic-units-per-second monotonic-units-per-second)
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.memory-lease-store"))))

(defun %memory-lease-now (store)
  (%now :clock (memory-lease-store-clock store)
        :monotonic-units-per-second
        (memory-lease-store-monotonic-units-per-second store)))

(defun %lease-record-matches-p (record lease now)
  (and record
       (equal (%lease-record-owner record) (resilience-lease-owner lease))
       (eql (%lease-record-fencing-token record)
            (resilience-lease-fencing-token lease))
       (> (%lease-record-expires-at record) now)))

(defmethod acquire-resilience-lease
    ((store memory-lease-store) key owner
     &key (ttl 30d0) (signal-on-unavailable-p t))
  (%ensure-positive-real ttl "TTL")
  (when (null owner)
    (error "LEASE owner must not be NIL."))
  (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
    (let* ((now (%memory-lease-now store))
           (leases (%memory-lease-store-leases store))
           (current (gethash key leases)))
      (cond
        ((and current
              (> (%lease-record-expires-at current) now)
              (not (equal owner (%lease-record-owner current))))
         (if signal-on-unavailable-p
             (error 'resilience-lease-unavailable
                    :key key
                    :owner owner
                    :retry-after
                    (max 0d0 (- (%lease-record-expires-at current) now))
                    :message "The resilience lease is held by another owner.")
             nil))
        (t
         (let* ((same-owner-p
                  (and current
                       (> (%lease-record-expires-at current) now)
                       (equal owner (%lease-record-owner current))))
                (fencing-token
                  (if same-owner-p
                      (%lease-record-fencing-token current)
                      (let ((next
                              (1+ (gethash key
                                          (%memory-lease-store-next-fencing-token store)
                                          0))))
                        (setf (gethash key
                                       (%memory-lease-store-next-fencing-token store))
                              next)
                        next)))
                (expires-at (+ now (float ttl 1d0))))
           (setf (gethash key leases)
                 (%make-lease-record owner fencing-token expires-at))
           (make-instance 'resilience-lease
                          :store store
                          :key key
                          :owner owner
                          :fencing-token fencing-token
                          :expires-at expires-at
                          :ttl (float ttl 1d0))))))))

(defmethod resilience-lease-held-p ((lease resilience-lease))
  (let ((store (resilience-lease-store lease)))
    (check-type store memory-lease-store)
    (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
      (%lease-record-matches-p
       (gethash (resilience-lease-key lease)
                (%memory-lease-store-leases store))
       lease
       (%memory-lease-now store)))))

(defmethod renew-resilience-lease ((lease resilience-lease) &key ttl)
  (let ((store (resilience-lease-store lease))
        (active-ttl (or ttl (resilience-lease-ttl lease))))
    (check-type store memory-lease-store)
    (%ensure-positive-real active-ttl "TTL")
    (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
      (let* ((now (%memory-lease-now store))
             (key (resilience-lease-key lease))
             (record (gethash key (%memory-lease-store-leases store))))
        (unless (%lease-record-matches-p record lease now)
          (error 'resilience-lease-lost
                 :key key
                 :owner (resilience-lease-owner lease)
                 :fencing-token (resilience-lease-fencing-token lease)
                 :message "The resilience lease can no longer be renewed."))
        (let ((expires-at (+ now (float active-ttl 1d0))))
          (setf (gethash key (%memory-lease-store-leases store))
                (%make-lease-record
                 (resilience-lease-owner lease)
                 (resilience-lease-fencing-token lease)
                 expires-at)
                (resilience-lease-expires-at lease) expires-at
                (resilience-lease-ttl lease) (float active-ttl 1d0))
          lease)))))

(defmethod release-resilience-lease
    ((lease resilience-lease) &key (ignore-lost-p nil))
  (let ((store (resilience-lease-store lease)))
    (check-type store memory-lease-store)
    (cl-concurrent-kit:with-lock-held ((%memory-lease-store-lock store))
      (let* ((key (resilience-lease-key lease))
             (record (gethash key (%memory-lease-store-leases store))))
        (if (and record
                 (equal (%lease-record-owner record)
                        (resilience-lease-owner lease))
                 (eql (%lease-record-fencing-token record)
                      (resilience-lease-fencing-token lease)))
            (progn
              (remhash key (%memory-lease-store-leases store))
              t)
            (if ignore-lost-p
                nil
                (error 'resilience-lease-lost
                       :key key
                       :owner (resilience-lease-owner lease)
                       :fencing-token (resilience-lease-fencing-token lease)
                       :message "The resilience lease is no longer owned.")))))))

(defmacro with-resilience-lease
    ((lease store key owner &rest options) &body body)
  "Acquire LEASE for BODY and release it on every exit path."
  `(let ((,lease (acquire-resilience-lease ,store ,key ,owner
                                            ,@options)))
     (when ,lease
       (unwind-protect
            (progn ,@body)
         (release-resilience-lease ,lease :ignore-lost-p t)))))

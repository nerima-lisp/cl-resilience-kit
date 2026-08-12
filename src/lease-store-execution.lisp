(in-package #:resilience-kit)

(defun make-memory-lease-store
    (&key clock monotonic-units-per-second name)
  (make-instance
   'memory-lease-store
   :clock (%active-clock clock)
   :monotonic-units-per-second
   (%active-monotonic-units-per-second monotonic-units-per-second)
   :lock (make-lock
          :name (or name "cl-resilience-kit.memory-lease-store"))))

(defun %memory-lease-now (store)
  (%now :clock (memory-lease-store-clock store)
        :monotonic-units-per-second
        (memory-lease-store-monotonic-units-per-second store)))

(defun %lease-record-held-p (record now)
  (and record
       (> (%lease-record-expires-at record) now)))

(defun %lease-record-owned-by-p (record owner)
  (and record
       (equal (%lease-record-owner record) owner)))

(defun %lease-record-owned-by-lease-p (record lease)
  (and (%lease-record-owned-by-p record (resilience-lease-owner lease))
       (eql (%lease-record-fencing-token record)
            (resilience-lease-fencing-token lease))))

(defun %lease-record-matches-p (record lease now)
  (and (%lease-record-held-p record now)
       (%lease-record-owned-by-lease-p record lease)))

(defun %ensure-lease-owner (owner)
  (when (null owner)
    (error "LEASE owner must not be NIL.")))

(defun %lease-record-retry-after (record now)
  (max 0d0 (- (%lease-record-expires-at record) now)))

(defun %memory-lease-next-fencing-token (store key)
  (let ((next (1+ (gethash key
                           (%memory-lease-store-next-fencing-token store)
                           0))))
    (setf (gethash key (%memory-lease-store-next-fencing-token store))
          next)
    next))

(defun %memory-lease-fencing-token (store key current owner now)
  (if (and (%lease-record-held-p current now)
           (%lease-record-owned-by-p current owner))
      (%lease-record-fencing-token current)
      (%memory-lease-next-fencing-token store key)))

(defun %memory-lease-expires-at (now ttl)
  (+ now (float ttl 1d0)))

(defun %make-resilience-lease (store key owner fencing-token expires-at ttl)
  (make-instance 'resilience-lease
                 :store store
                 :key key
                 :owner owner
                 :fencing-token fencing-token
                 :expires-at expires-at
                 :ttl (float ttl 1d0)))

(defun %signal-lease-unavailable (key owner current now)
  (error 'resilience-lease-unavailable
         :key key
         :owner owner
         :retry-after (%lease-record-retry-after current now)
         :message "The resilience lease is held by another owner."))

(defun %signal-lease-lost (lease message)
  (error 'resilience-lease-lost
         :key (resilience-lease-key lease)
         :owner (resilience-lease-owner lease)
         :fencing-token (resilience-lease-fencing-token lease)
         :message message))

(defmethod acquire-resilience-lease
    ((store memory-lease-store) key owner
     &key (ttl 30d0) (signal-on-unavailable-p t))
  (%ensure-positive-real ttl "TTL")
  (%ensure-lease-owner owner)
  (with-lock-held ((%memory-lease-store-lock store))
    (let* ((now (%memory-lease-now store))
           (leases (%memory-lease-store-leases store))
           (current (gethash key leases)))
      (cond
        ((and (%lease-record-held-p current now)
              (not (%lease-record-owned-by-p current owner)))
         (when signal-on-unavailable-p
           (%signal-lease-unavailable key owner current now)))
        (t
         (let* ((fencing-token
                  (%memory-lease-fencing-token store key current owner now))
                (expires-at (%memory-lease-expires-at now ttl)))
           (setf (gethash key leases)
                 (%make-lease-record owner fencing-token expires-at))
           (%make-resilience-lease
            store key owner fencing-token expires-at ttl)))))))

(defmethod resilience-lease-held-p ((lease resilience-lease))
  (let ((store (resilience-lease-store lease)))
    (check-type store memory-lease-store)
    (with-lock-held ((%memory-lease-store-lock store))
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
    (with-lock-held ((%memory-lease-store-lock store))
      (let* ((now (%memory-lease-now store))
             (key (resilience-lease-key lease))
             (record (gethash key (%memory-lease-store-leases store))))
        (unless (%lease-record-matches-p record lease now)
          (%signal-lease-lost
           lease
           "The resilience lease can no longer be renewed."))
        (let ((expires-at (%memory-lease-expires-at now active-ttl)))
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
    (with-lock-held ((%memory-lease-store-lock store))
      (let* ((key (resilience-lease-key lease))
             (record (gethash key (%memory-lease-store-leases store))))
        (if (%lease-record-owned-by-lease-p record lease)
            (progn
              (remhash key (%memory-lease-store-leases store))
              t)
            (if ignore-lost-p
                nil
                (%signal-lease-lost
                 lease
                 "The resilience lease is no longer owned.")))))))

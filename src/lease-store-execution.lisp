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
  (%monotonic-seconds
   (%memory-lease-store-clock store)
   (%memory-lease-store-monotonic-units-per-second store)))

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

(defun %lease-record-matches-values-p (record owner fencing-token now)
  (and (%lease-record-held-p record now)
       (equal (%lease-record-owner record) owner)
       (eql (%lease-record-fencing-token record) fencing-token)))

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
  (let ((normalized-ttl (float ttl 1d0)))
    (with-lock-held ((%memory-lease-store-lock store))
      (let* ((now (%memory-lease-now store))
             (leases (%memory-lease-store-leases store))
             (next-fencing-tokens
               (%memory-lease-store-next-fencing-token store))
             (current (gethash key leases)))
        (cond
          (current
           (let ((current-owner (%lease-record-owner current))
                 (current-expires-at (%lease-record-expires-at current)))
             (if (and (> current-expires-at now)
                      (not (equal owner current-owner)))
                 (if signal-on-unavailable-p
                     (%signal-lease-unavailable key owner current now)
                     nil)
                 (let* ((same-owner-p
                          (and (> current-expires-at now)
                               (equal owner current-owner)))
                        (fencing-token
                          (if same-owner-p
                              (%lease-record-fencing-token current)
                              (let ((next
                                      (1+ (gethash key next-fencing-tokens 0))))
                                (setf (gethash key next-fencing-tokens) next)
                                next)))
                        (expires-at (+ now normalized-ttl)))
                   (setf (gethash key leases)
                         (%make-lease-record owner fencing-token expires-at))
                   (%make-resilience-lease
                    store key owner fencing-token expires-at normalized-ttl)))))
          (t
           (let* ((fencing-token
                    (let ((next (1+ (gethash key next-fencing-tokens 0))))
                      (setf (gethash key next-fencing-tokens) next)
                      next))
                  (expires-at (+ now normalized-ttl)))
             (setf (gethash key leases)
                   (%make-lease-record owner fencing-token expires-at))
             (%make-resilience-lease
              store key owner fencing-token expires-at normalized-ttl))))))))

(defmethod resilience-lease-held-p ((lease resilience-lease))
  (let ((store (%resilience-lease-store lease))
        (key (%resilience-lease-key lease))
        (owner (%resilience-lease-owner lease))
        (fencing-token (%resilience-lease-fencing-token lease)))
    (check-type store memory-lease-store)
    (with-lock-held ((%memory-lease-store-lock store))
      (let ((leases (%memory-lease-store-leases store)))
        (%lease-record-matches-values-p
         (gethash key leases)
         owner
         fencing-token
         (%memory-lease-now store))))))

(defmethod renew-resilience-lease ((lease resilience-lease) &key ttl)
  (let ((store (%resilience-lease-store lease))
        (active-ttl (or ttl (%resilience-lease-ttl lease))))
    (check-type store memory-lease-store)
    (%ensure-positive-real active-ttl "TTL")
    (let ((normalized-ttl (float active-ttl 1d0)))
      (with-lock-held ((%memory-lease-store-lock store))
        (let* ((now (%memory-lease-now store))
               (key (%resilience-lease-key lease))
               (owner (%resilience-lease-owner lease))
               (fencing-token (%resilience-lease-fencing-token lease))
               (leases (%memory-lease-store-leases store))
               (record (gethash key leases)))
          (unless (%lease-record-matches-values-p
                   record owner fencing-token now)
            (%signal-lease-lost
             lease
             "The resilience lease can no longer be renewed."))
          (let ((expires-at (+ now normalized-ttl)))
            (setf (gethash key leases)
                  (%make-lease-record owner fencing-token expires-at)
                  (%resilience-lease-expires-at lease) expires-at
                  (%resilience-lease-ttl lease) normalized-ttl)
            lease))))))

(defmethod release-resilience-lease
    ((lease resilience-lease) &key (ignore-lost-p nil))
  (let ((store (%resilience-lease-store lease)))
    (check-type store memory-lease-store)
    (with-lock-held ((%memory-lease-store-lock store))
      (let* ((key (%resilience-lease-key lease))
             (owner (%resilience-lease-owner lease))
             (fencing-token (%resilience-lease-fencing-token lease))
             (leases (%memory-lease-store-leases store))
             (record (gethash key leases)))
        (if (and record
                 (equal (%lease-record-owner record) owner)
                 (eql (%lease-record-fencing-token record) fencing-token))
            (progn
              (remhash key leases)
              t)
            (if ignore-lost-p
                nil
                (%signal-lease-lost
                 lease
                 "The resilience lease is no longer owned.")))))))

(in-package #:resilience-kit)

(defclass resilience-lease-store () ())

(defgeneric acquire-resilience-lease
    (store key owner &key ttl signal-on-unavailable-p))
(defgeneric renew-resilience-lease (lease &key ttl))
(defgeneric release-resilience-lease (lease &key ignore-lost-p))
(defgeneric resilience-lease-held-p (lease))

(defclass resilience-lease ()
  ((store
    :initarg :store
    :reader resilience-lease-store
    :reader %resilience-lease-store)
   (key
    :initarg :key
    :reader resilience-lease-key
    :reader %resilience-lease-key)
   (owner
    :initarg :owner
    :reader resilience-lease-owner
    :reader %resilience-lease-owner)
   (fencing-token
    :initarg :fencing-token
    :reader resilience-lease-fencing-token
    :reader %resilience-lease-fencing-token)
   (expires-at
    :initarg :expires-at
    :accessor resilience-lease-expires-at
    :accessor %resilience-lease-expires-at)
   (ttl
    :initarg :ttl
    :accessor resilience-lease-ttl
    :accessor %resilience-lease-ttl)))

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
    :reader memory-lease-store-clock
    :reader %memory-lease-store-clock)
   (monotonic-units-per-second
    :initarg :monotonic-units-per-second
    :reader memory-lease-store-monotonic-units-per-second
    :reader %memory-lease-store-monotonic-units-per-second)))

(in-package #:resilience-kit)

(define-condition resilience-store-error (resilience-error)
  ((key
    :initarg :key
    :reader resilience-store-error-key)
   (cause
    :initarg :cause
    :initform nil
    :reader resilience-store-error-cause)))

(define-condition resilience-store-conflict (resilience-store-error)
  ((expected-version
    :initarg :expected-version
    :initform nil
    :reader resilience-store-conflict-expected-version)
   (actual-version
    :initarg :actual-version
    :initform nil
    :reader resilience-store-conflict-actual-version)))

(define-condition resilience-lease-unavailable (resilience-error)
  ((key
    :initarg :key
    :reader resilience-lease-unavailable-key)
   (owner
    :initarg :owner
    :reader resilience-lease-unavailable-owner)
   (retry-after
    :initarg :retry-after
    :initform nil
    :reader resilience-lease-unavailable-retry-after)))

(define-condition resilience-lease-lost (resilience-error)
  ((key
    :initarg :key
    :reader resilience-lease-lost-key)
   (owner
    :initarg :owner
    :reader resilience-lease-lost-owner)
   (fencing-token
    :initarg :fencing-token
    :initform nil
    :reader resilience-lease-lost-fencing-token)))

(define-condition stale-fencing-token (resilience-error)
  ((key
    :initarg :key
    :reader stale-fencing-token-key)
   (fencing-token
    :initarg :fencing-token
    :reader stale-fencing-token-fencing-token)
   (current-fencing-token
    :initarg :current-fencing-token
    :reader stale-fencing-token-current-fencing-token)))

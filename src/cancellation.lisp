(in-package #:resilience-kit)

(defclass cancellation-token ()
  ((parent
    :initarg :parent
    :initform nil
    :reader cancellation-token-parent)
   (%cancelled-p
    :initform nil
    :accessor %cancellation-token-cancelled-p)
   (%reason
    :initform nil
    :accessor %cancellation-token-reason)
   (%lock
    :initarg :lock
    :reader %cancellation-token-lock)))

(defun make-cancellation-token (&key parent)
  "Create a cooperative cancellation token, optionally linked to PARENT.

Cancellation is observed only when CHECK-CANCELLATION-TOKEN is called.  A
parent token's cancellation is visible to every descendant token."
  (when parent
    (check-type parent cancellation-token))
  (make-instance
   'cancellation-token
   :parent parent
   :lock (make-lock :name "cl-resilience-kit.cancellation-token")))

(defun %cancellation-token-local-state (token)
  (with-lock-held ((%cancellation-token-lock token))
    (values (%cancellation-token-cancelled-p token)
            (%cancellation-token-reason token))))

(defun cancellation-token-cancelled-p (token)
  "Return true when TOKEN or one of its parents has been cancelled."
  (check-type token cancellation-token)
  (multiple-value-bind (cancelled-p reason)
      (%cancellation-token-local-state token)
    (declare (ignore reason))
    (or cancelled-p
        (let ((parent (cancellation-token-parent token)))
          (and parent (cancellation-token-cancelled-p parent))))))

(defun cancellation-token-reason (token)
  "Return the local cancellation reason, or the nearest parent reason."
  (check-type token cancellation-token)
  (multiple-value-bind (cancelled-p reason)
      (%cancellation-token-local-state token)
    (if cancelled-p
        reason
        (let ((parent (cancellation-token-parent token)))
          (and parent (cancellation-token-reason parent))))))

(defun cancel-cancellation-token (token &rest arguments)
  "Cancel TOKEN and return it.

The optional reason may be supplied positionally or as `:REASON`.  Repeated
cancellation preserves the first reason, which makes parent propagation
stable for observers."
  (check-type token cancellation-token)
  (let ((reason
          (cond ((null arguments) :cancelled)
                ((= (length arguments) 1) (first arguments))
                ((and (= (length arguments) 2)
                      (eq (first arguments) :reason))
                 (second arguments))
                (t
                 (error "CANCEL-CANCELLATION-TOKEN accepts an optional reason.")))))
    (with-lock-held ((%cancellation-token-lock token))
      (unless (%cancellation-token-cancelled-p token)
        (setf (%cancellation-token-cancelled-p token) t
              (%cancellation-token-reason token) reason)))
    token))

(defun check-cancellation-token (token)
  "Signal RESILIENCE-CANCELLED when TOKEN or an ancestor is cancelled."
  (check-type token cancellation-token)
  (when (cancellation-token-cancelled-p token)
    (error 'resilience-cancelled
           :message (format nil "The resilience operation was cancelled~@[ (~A)~]."
                            (cancellation-token-reason token))
           :token token
           :reason (cancellation-token-reason token)))
  nil)

(defun %active-cancellation-token (token)
  (or token *resilience-cancellation-token*))

(defun %check-active-cancellation-token ()
  (when *resilience-cancellation-token*
    (check-cancellation-token *resilience-cancellation-token*)))

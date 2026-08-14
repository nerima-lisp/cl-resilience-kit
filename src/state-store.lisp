(in-package #:resilience-kit)

;;; Portable state store and fencing lease contracts

(defclass resilience-state-store () ())

(defgeneric state-store-get (store key))
(defgeneric state-store-put-if-version (store key value expected-version))
(defgeneric state-store-delete-if-version (store key expected-version))
(defgeneric state-store-scan-prefix (store prefix))

(defmethod state-store-get ((store resilience-state-store) key)
  (error 'resilience-store-error
         :key key
         :message "The state store has no GET implementation."))

(defmethod state-store-put-if-version
    ((store resilience-state-store) key value expected-version)
  (declare (ignore value expected-version))
  (error 'resilience-store-error
         :key key
         :message "The state store has no compare-and-set implementation."))

(defmethod state-store-delete-if-version
    ((store resilience-state-store) key expected-version)
  (declare (ignore expected-version))
  (error 'resilience-store-error
         :key key
         :message "The state store has no compare-and-delete implementation."))

(defmethod state-store-scan-prefix ((store resilience-state-store) prefix)
  (declare (ignore prefix))
  (error 'resilience-store-error
         :key nil
         :message "The state store has no prefix-scan implementation."))

(defstruct (%state-record
            (:constructor %make-state-record (version value)))
  version
  value)

(defclass memory-state-store (resilience-state-store)
  ((%records
    :initform (make-hash-table :test #'equal)
    :reader %memory-state-store-records)
   (%lock
    :initarg :lock
    :reader %memory-state-store-lock)))

(defun make-memory-state-store (&key name)
  (make-instance
   'memory-state-store
   :lock (cl-concurrent-kit:make-lock
          :name (or name "cl-resilience-kit.memory-state-store"))))

(defmethod state-store-get ((store memory-state-store) key)
  (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (let ((record (gethash key (%memory-state-store-records store))))
      (if record
          (values (copy-tree (%state-record-value record))
                  (%state-record-version record))
          (values nil nil)))))

(defmethod state-store-put-if-version
    ((store memory-state-store) key value expected-version)
  (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (let* ((records (%memory-state-store-records store))
           (record (gethash key records))
           (actual-version (and record (%state-record-version record))))
      (unless (eql expected-version actual-version)
        (error 'resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version actual-version
               :message "The state-store compare-and-set precondition failed."))
      (let ((new-version (1+ (or actual-version 0))))
        (setf (gethash key records)
              (%make-state-record new-version (copy-tree value)))
        new-version))))

(defmethod state-store-delete-if-version
    ((store memory-state-store) key expected-version)
  (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
    (let* ((records (%memory-state-store-records store))
           (record (gethash key records))
           (actual-version (and record (%state-record-version record))))
      (unless (eql expected-version actual-version)
        (error 'resilience-store-conflict
               :key key
               :expected-version expected-version
               :actual-version actual-version
               :message "The state-store compare-and-delete precondition failed."))
      (remhash key records)
      t)))

(defmethod state-store-scan-prefix ((store memory-state-store) prefix)
  (check-type prefix string)
  (let ((prefix-length (length prefix)))
    (cl-concurrent-kit:with-lock-held ((%memory-state-store-lock store))
      (let ((records (%memory-state-store-records store)))
        (loop for key being the hash-keys of records
              using (hash-value record)
          when (and (stringp key)
                    (<= prefix-length (length key))
                    (string= prefix key :end2 prefix-length))
                collect (list key
                              (copy-tree (%state-record-value record))
                              (%state-record-version record)))))))

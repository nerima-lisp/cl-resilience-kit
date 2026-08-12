(in-package #:resilience-kit)

(defclass request-coalescer ()
  ((lock
    :initform (make-lock
               :name "resilience-request-coalescer")
    :reader %request-coalescer-lock)
   (entries
    :initform (make-hash-table :test #'equal)
    :reader %request-coalescer-entries)))

(defun make-request-coalescer ()
  (make-instance 'request-coalescer))

(defun request-coalescer-size (coalescer)
  (check-type coalescer request-coalescer)
  (with-lock-held ((%request-coalescer-lock coalescer))
    (hash-table-count (%request-coalescer-entries coalescer))))

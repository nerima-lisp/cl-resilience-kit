(in-package #:resilience-kit)

(defun %proper-list-p (value)
  "Return true when VALUE is a finite, proper list.

LISTP alone accepts dotted and circular lists.  These records cross state
store boundaries, so rejecting malformed lists before traversal is part of
the data boundary rather than an incidental property of the consumers."
  (or (null value)
      (and (listp value)
           (handler-case
               (list-length value)
             (type-error () nil)))))

(defun %validate-static-plist-options (options option-keys owner)
  "Validate a literal plist OPTIONS against OPTION-KEYS for OWNER.

OWNER is used only in condition messages and is typically the validating
macro name."
  (let ((length (list-length options)))
    (unless (or (null options) length)
      (error "~S options must be a proper property list: ~S"
             owner options))
    (let ((length (or length 0)))
      (unless (evenp length)
        (error "~S options must contain keyword/value pairs: ~S"
               owner options))))
  (loop with allowed = (make-hash-table :test #'eq)
        with seen = (make-hash-table :test #'eq)
        initially (dolist (key option-keys)
                    (setf (gethash key allowed) t))
        for key in options by #'cddr
        do (unless (keywordp key)
             (error "~S option keys must be keywords: ~S"
                    owner key))
           (unless (gethash key allowed)
             (error "Unknown ~S option ~S" owner key))
           (when (gethash key seen)
             (error "Duplicate ~S option ~S" owner key))
           (setf (gethash key seen) t))
  t)

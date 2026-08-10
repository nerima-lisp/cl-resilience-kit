(in-package #:cl-resilience-kit)

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

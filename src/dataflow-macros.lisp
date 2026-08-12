(in-package #:resilience-dataflow)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +resilience-option-keys+
    '(:resilience-options))

  (defun %resilience-node-clause-consumed-key-p (key)
    (or (eq key :handler)
        (member key +resilience-option-keys+)))

  (defun %resilience-pipeline-consumed-key-p (key)
    (member key +resilience-option-keys+))

  (defun %remove-plist-keys (plist predicate)
    (loop for (key value) on plist by #'cddr
          unless (funcall predicate key)
            append (list key value)))

  (defun %resilience-node-clause-p (clause)
    (and (consp clause)
         (eq (first clause) :node)))

  (defun %node-handler-form (plist missing)
    (getf plist :handler missing))

  (defun %node-resilience-options-form (plist missing)
    (getf plist :resilience-options missing))

  (defun %pipeline-resilience-options-form (plist missing)
    (getf plist :resilience-options missing))

  (defun %reject-legacy-resilience-alias (plist context)
    (let ((missing (gensym "MISSING-LEGACY-RESILIENCE")))
      (unless (eq (getf plist :resilience missing) missing)
        (error
         "define-resilience-pipeline does not accept :RESILIENCE in ~A; use :RESILIENCE-OPTIONS instead"
         context))))

  (defun %effective-resilience-options-form
      (default-resilience-options node-resilience-options missing)
    (cond ((eq default-resilience-options missing)
           (unless (eq node-resilience-options missing)
             node-resilience-options))
          ((eq node-resilience-options missing)
           default-resilience-options)
          (t
           `(%merge-resilience-options
             ,default-resilience-options
             ,node-resilience-options))))

  (defun %rewrite-resilience-node-clause
      (clause missing default-resilience-options)
    (destructuring-bind (kind name &rest plist) clause
      (declare (ignore kind))
      (%reject-legacy-resilience-alias plist clause)
      (let ((handler (%node-handler-form plist missing))
            (resilience-options
              (%node-resilience-options-form plist missing)))
        (when (eq handler missing)
          (error "define-resilience-pipeline requires :handler in ~S"
                 clause))
        `(:node ,name
           ,@(%remove-plist-keys
              plist
              #'%resilience-node-clause-consumed-key-p)
           :handler
           (%resilience-handler
            ,handler
            ,(%effective-resilience-options-form
              default-resilience-options
              resilience-options
              missing))))))

  (defun %rewrite-resilience-clauses
      (clauses missing default-resilience-options)
    (mapcar (lambda (clause)
              (if (%resilience-node-clause-p clause)
                  (%rewrite-resilience-node-clause
                   clause
                   missing
                   default-resilience-options)
                  clause))
            clauses)))

(defmacro define-resilience-pipeline ((&rest options) &body clauses)
  "Expand to define-pipeline while wrapping every :node handler
with resilience-kit:call-with-resilience.

The top-level options list may provide :RESILIENCE-OPTIONS as a default for
all nodes. Each :node clause may also provide :RESILIENCE-OPTIONS, which
overrides the top-level defaults. This keyword is consumed by this macro and
not forwarded to the underlying dataflow pipeline builder."
  (let* ((missing (gensym "MISSING-OPTION"))
         (default-resilience-options
           (%pipeline-resilience-options-form options missing))
         (pipeline-options
           (%remove-plist-keys options #'%resilience-pipeline-consumed-key-p)))
    (%reject-legacy-resilience-alias options options)
    `(define-pipeline (,@pipeline-options)
       ,@(%rewrite-resilience-clauses
          clauses
          missing
          default-resilience-options))))

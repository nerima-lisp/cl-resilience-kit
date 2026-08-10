(require :asdf)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (load (merge-pathnames
         "scripts/bootstrap.lisp"
         (uiop:pathname-directory-pathname
          (or *load-truename* *compile-file-truename*)))))

(let ((root (uiop:pathname-directory-pathname *load-truename*)))
  (bootstrap-cl-resilience-kit root)
  (asdf:test-system "cl-resilience-kit/test"))

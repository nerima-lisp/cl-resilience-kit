(require :asdf)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (load (merge-pathnames
         "scripts/bootstrap.lisp"
         (uiop:pathname-directory-pathname
          (or *load-truename* *compile-file-truename*)))))

(let ((root (uiop:pathname-directory-pathname *load-truename*)))
  (bootstrap-cl-resilience-kit root)
  ;; Target the full suite directly rather than "cl-resilience-kit" (whose
  ;; :in-order-to redirects test-op here): a direct target still fails loudly
  ;; via ASDF dependency resolution if a subsystem is missing, without
  ;; depending on that redirect continuing to exist on the main system.
  (asdf:test-system "cl-resilience-kit/all-test"))

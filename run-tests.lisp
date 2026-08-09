(require :asdf)

(let ((root (uiop:pathname-directory-pathname *load-truename*)))
  (pushnew root asdf:*central-registry* :test #'equal)
  (unless (uiop:getenv "CL_SOURCE_REGISTRY")
    (dolist (directory
              (list (merge-pathnames "../cl-boundary-kit/" root)
                    (merge-pathnames "../cl-concurrent-kit/" root)
                    (merge-pathnames "../cl-date-kit/" root)
                    (merge-pathnames "../cl-weave/" root)))
      (when (uiop:directory-exists-p directory)
        (pushnew directory asdf:*central-registry* :test #'equal))))
  (asdf:test-system "cl-resilience-kit/test"))

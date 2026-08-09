(require :asdf)

(let ((root (uiop:pathname-directory-pathname *load-truename*)))
  (dolist (directory (list root
                           (merge-pathnames "../cl-boundary-kit/" root)
                           (merge-pathnames "../cl-concurrent-kit/" root)
                           (merge-pathnames "../cl-date-kit/" root)
                           (merge-pathnames "../cl-host-kit/" root)
                           (merge-pathnames "../cl-weave/" root)))
    (pushnew directory asdf:*central-registry* :test #'equal))
  (asdf:test-system "cl-resilience-kit/test"))

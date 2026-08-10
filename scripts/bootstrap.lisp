(in-package #:cl-user)

(defun bootstrap-cl-resilience-kit (root)
  "Register the project and adjacent NERIMA systems for a local run.

The Nix checks provide dependencies through their isolated source registry.
This bootstrap additionally discovers the sibling checkouts used by local
development, including transitive systems such as CL-HOST-KIT."
  (require :asdf)
  (let ((directories
          (remove-if-not
           #'uiop:directory-exists-p
           (mapcar (lambda (name)
                     (merge-pathnames (format nil "../~A/" name) root))
                   '("cl-boundary-kit"
                     "cl-concurrent-kit"
                     "cl-date-kit"
                     "cl-host-kit"
                     "cl-observability-kit"
                     "cl-weave")))))
    (asdf:initialize-source-registry
     `(:source-registry
       (:tree ,root)
       ,@(mapcar (lambda (directory) `(:tree ,directory)) directories)
       :inherit-configuration))
    (pushnew root asdf:*central-registry* :test #'equal)
    (dolist (directory directories)
      (pushnew directory asdf:*central-registry* :test #'equal)))
  root)

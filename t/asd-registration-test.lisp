(in-package #:resilience-kit/test)

;;; Compare ASDF's registered test components with files on disk. The runner
;;; rejects only an entirely empty plan, so an unregistered test can otherwise
;;; disappear silently; the union covers all test systems rooted at t/.

(defparameter +t-directory-check-systems+
  '("cl-resilience-kit/test"
    "cl-resilience-kit/observability-test"
    "cl-resilience-kit/dataflow-test")
  "Every ASDF system whose components are rooted at the t/ directory.")

(defun %registered-t-file-names ()
  "Return the file-namestrings of every :file component registered across
   +T-DIRECTORY-CHECK-SYSTEMS+, asking ASDF's own component tree rather than
   parsing the .asd source text."
  (let ((names nil))
    (dolist (system-name +t-directory-check-systems+)
      (dolist (child (asdf:component-children (asdf:find-system system-name)))
        (when (typep child 'asdf:cl-source-file)
          (pushnew (file-namestring (asdf:component-pathname child))
                   names
                   :test #'string=))))
    names))

(defun %t-directory-pathname ()
  "Return the absolute pathname of the t/ directory, derived from the
   cl-resilience-kit/test system's own :pathname rather than a literal path
   -- the source tree lives at a different location inside the Nix build
   sandbox than it does in a working checkout."
  (asdf:component-pathname (asdf:find-system "cl-resilience-kit/test")))

(defun %on-disk-t-file-names ()
  "Return the file-namestrings of every *.lisp file physically present in
   the t/ directory."
  (mapcar #'file-namestring
          (remove-if-not
           (lambda (pathname)
             (equal (pathname-type pathname) "lisp"))
           (uiop:directory-files (%t-directory-pathname)))))

(describe "t/ directory ASDF registration"
  (it "registers every t/*.lisp file as a component of a resilience-kit test system"
    (let* ((registered (%registered-t-file-names))
           (on-disk (%on-disk-t-file-names))
           (unregistered
             (sort (set-difference on-disk registered :test #'string=)
                   #'string<)))
      (expect unregistered :to-equal nil))))

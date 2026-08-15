(in-package #:resilience-kit/test)

;;; FR-005: t/runner.lisp:6-7 only errors when the selected test plan is
;;; COMPLETELY empty, so a file that sits in t/ but was never added to any
;;; system's :components list compiles, runs, and reports nothing -- and
;;; nothing else notices. This test closes that gap by asking ASDF itself,
;;; not a reimplementation of it, which source files it has registered
;;; across every test system rooted at t/, and comparing that set against
;;; what is physically on disk.
;;;
;;; A file may be registered in any one of the three test systems below
;;; (they all share :pathname "t"), so the check takes the union across all
;;; three rather than checking a single system.
;;;
;;; The reverse direction -- every registered component exists on disk -- is
;;; deliberately not asserted here. cl-resilience-kit/all-test depends on
;;; all three systems, and run-tests.lisp targets all-test, so ASDF's own
;;; component resolution already fails loudly (a file-error during load-op)
;;; for any registered-but-missing file before this test could ever run.
;;; Asserting it here would be an untestable branch: nothing on this entry
;;; point could make it fail.

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

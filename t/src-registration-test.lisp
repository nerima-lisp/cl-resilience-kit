(in-package #:resilience-kit/test)

;;; Compare ASDF's registered production components with files on disk. An
;;; unregistered src/*.lisp file is never compiled or loaded, so the union
;;; covers all production systems rooted at src/ and guards against silent
;;; code loss.
;;;
;;; External dependencies are resolved only when systems are operated on;
;;; FIND-SYSTEM can therefore inspect all three definitions here.
;;;
;;; Missing registered components are reported by ASDF before this test runs.

(defparameter +src-directory-check-systems+
  '("cl-resilience-kit"
    "cl-resilience-kit/observability"
    "cl-resilience-kit/dataflow")
  "Every ASDF system whose components are rooted at the src/ directory.")

(defun %registered-src-file-names ()
  "Return the file-namestrings of every :file component registered across
   +SRC-DIRECTORY-CHECK-SYSTEMS+, asking ASDF's own component tree rather
   than parsing the .asd source text."
  (let ((names nil))
    (dolist (system-name +src-directory-check-systems+)
      (dolist (child (asdf:component-children (asdf:find-system system-name)))
        (when (typep child 'asdf:cl-source-file)
          (pushnew (file-namestring (asdf:component-pathname child))
                   names
                   :test #'string=))))
    names))

(defun %src-directory-pathname ()
  "Return the absolute pathname of the src/ directory, derived from the
   cl-resilience-kit system's own :pathname rather than a literal path --
   the source tree lives at a different location inside the Nix build
   sandbox than it does in a working checkout."
  (asdf:component-pathname (asdf:find-system "cl-resilience-kit")))

(defun %on-disk-src-file-names ()
  "Return the file-namestrings of every *.lisp file physically present in
   the src/ directory."
  (mapcar #'file-namestring
          (remove-if-not
           (lambda (pathname)
             (equal (pathname-type pathname) "lisp"))
           (uiop:directory-files (%src-directory-pathname)))))

(describe "src/ directory ASDF registration"
  (it "registers every src/*.lisp file as a component of a resilience-kit production system"
    (let* ((registered (%registered-src-file-names))
           (on-disk (%on-disk-src-file-names))
           (unregistered
             (sort (set-difference on-disk registered :test #'string=)
                   #'string<)))
      (expect unregistered :to-equal nil))))

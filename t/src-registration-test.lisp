(in-package #:resilience-kit/test)

;;; src/ counterpart to t/asd-registration-test.lisp. That test closed a
;;; hole on the t/ side: t/runner.lisp:6-7 only errors when the selected
;;; test plan is COMPLETELY empty, so a file that sits in t/ but was never
;;; added to any system's :components list compiles, runs, and reports
;;; nothing. The same hole is open on the src/ side, and it is worse: an
;;; unregistered src/*.lisp file is production code that compiles never,
;;; loads never, and nothing reports it -- silent code loss, with no
;;; empty-test-plan check standing between it and total silence. This test
;;; closes that gap by asking ASDF itself, not a reimplementation of it,
;;; which source files it has registered across every production system
;;; rooted at src/, and comparing that set against what is physically on
;;; disk.
;;;
;;; A file may be registered in any one of the three src/-rooted systems
;;; below (they all share :pathname "src"), so the check takes the union
;;; across all three rather than checking a single system.
;;;
;;; CL-RESILIENCE-KIT/OBSERVABILITY and CL-RESILIENCE-KIT/DATAFLOW are only
;;; *loadable* when their external dependencies (cl-observability-kit,
;;; cl-dataflow-kit) are installed. That does not threaten ASDF:FIND-SYSTEM
;;; below: FIND-SYSTEM only needs a system to be *defined*, and all three
;;; systems are defined unconditionally by defsystem forms living in the
;;; same cl-resilience-kit.asd file that ASDF already had to locate and
;;; read to find CL-RESILIENCE-KIT/TEST (this test system's own
;;; dependency). :depends-on is resolved only when a system is operated on
;;; (load-op, compile-op), never when it is merely found, so the possible
;;; absence of cl-observability-kit or cl-dataflow-kit cannot make
;;; ASDF:FIND-SYSTEM signal here. Guarding this call with
;;; (asdf:find-system name nil) and skipping an absent system would risk
;;; exactly the false positive this test exists to prevent -- reporting a
;;; fully-registered file as unregistered because the system that
;;; registers it was skipped -- so this test deliberately uses the
;;; erroring FIND-SYSTEM rather than defend against a failure mode that
;;; cannot occur here.
;;;
;;; The reverse direction -- every registered component exists on disk --
;;; is deliberately not asserted here, for the same reason
;;; t/asd-registration-test.lisp omits it: cl-resilience-kit/all-test
;;; depends, transitively through cl-resilience-kit/observability-test and
;;; cl-resilience-kit/dataflow-test, on all three systems checked below,
;;; and run-tests.lisp targets all-test, so ASDF's own component
;;; resolution already fails loudly (a file-error during load-op) for any
;;; registered-but-missing file before this test could ever run. Asserting
;;; it here would be an untestable branch: nothing on this entry point
;;; could make it fail.

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

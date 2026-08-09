(progn
  #.(progn (require :asdf) (require :sb-cover) nil)

  (defun script-directory ()
    (make-pathname :name nil
                   :type nil
                   :defaults (or *load-truename*
                                 *compile-file-truename*
                                 (error "Unable to determine the script location"))))

  (defun project-root ()
    (uiop:ensure-directory-pathname (script-directory)))

  (defun configure-local-source-registry (root)
    (asdf:initialize-source-registry
     `(:source-registry
       (:tree ,root)
       (:tree ,(merge-pathnames "../cl-boundary-kit/" root))
       (:tree ,(merge-pathnames "../cl-concurrent-kit/" root))
       (:tree ,(merge-pathnames "../cl-date-kit/" root))
       (:tree ,(merge-pathnames "../cl-host-kit/" root))
       (:tree ,(merge-pathnames "../cl-weave/" root))
       :inherit-configuration)))

  (defun coverage-arguments ()
    (remove "--" (uiop:command-line-arguments) :test #'string=))

  (defun output-directory (root)
    (let ((arguments (coverage-arguments)))
      (when (> (length arguments) 1)
        (error "Expected at most one coverage output directory, got ~S"
               arguments))
      (ensure-directories-exist
       (if arguments
           (uiop:ensure-directory-pathname
            (uiop:parse-native-namestring (first arguments)))
           (merge-pathnames "coverage/" root)))))

  (defun configure-isolated-output-cache (directory)
    ;; ASDF's absolute output translation flattens component names.  A unique
    ;; cache per run prevents unrelated systems' package.fasl files from
    ;; colliding when coverage is run repeatedly in the same output directory.
    (let* ((cache-name
             (format nil "asdf-cache-~D-~D/"
                     (get-universal-time)
                     (random most-positive-fixnum)))
           (cache (merge-pathnames cache-name directory)))
      (ensure-directories-exist cache)
      (asdf:initialize-output-translations
       `(:output-translations
         (t ,cache)
         :ignore-inherited-configuration))))

  (defun project-source-p (file root)
    (uiop:subpathp (uiop:parse-native-namestring file)
                   (merge-pathnames "src/" root)))

  (defun discard-ineligible-coverage-records (root)
    "Keep only this project's executable source records for LCOV.

SB-COVER:REPORT accepts a pathname filter, but SB-COVER:LCOV-REPORT walks
the complete coverage table.  Dependency records and declaration-only
PACKAGE.LISP records can therefore contain no source-location vector and
make LCOV generation fail even after the selected tests pass."
    (let ((table (sb-cover::code-coverage-hashtable))
          (discarded-files nil))
      (maphash (lambda (file coverage)
                 (declare (ignore coverage))
                 (unless (and (project-source-p file root)
                              (string/= (file-namestring file) "package.lisp"))
                   (push file discarded-files)))
               table)
      (dolist (file discarded-files)
        (remhash file table))))

  (defun run-coverage (root output)
    ;; LCOV needs the compiler's source-location vectors.  HTML reporting
    ;; can reconstruct locations from source files, but LCOV-REPORT cannot.
    ;; Enable the logging path before ASDF compiles the instrumented files.
    (sb-cover:enable-coverage-logging)
    ;; Instrument the project as ASDF compiles it.  PROCLAIM is used here
    ;; because this is a runtime script; DECLAIM is intended for declarations
    ;; processed while compiling a source file.
    (proclaim '(optimize (sb-cover:store-coverage-data 3)))
    (unwind-protect
         (progn
           ;; Load the test system in one top-level operation so ASDF cannot
           ;; silently compile the same component again after instrumentation
           ;; has been disabled.
           (asdf:test-system "cl-resilience-kit/test" :force t)
           (proclaim '(optimize (sb-cover:store-coverage-data 0)))
           (discard-ineligible-coverage-records root)
           (sb-cover:report (merge-pathnames "html/" output)
                            :if-matches (lambda (file)
                                          (project-source-p file root)))
           (sb-cover:lcov-report (merge-pathnames "lcov.info" output))
           (format t "Coverage reports written to ~A~%" output))
      ;; A failed test run must not leave coverage instrumentation enabled in
      ;; the same image, even though no report is written in that case.
      (proclaim '(optimize (sb-cover:store-coverage-data 0)))))

  (let* ((root (project-root))
         (output (output-directory root)))
    (configure-local-source-registry root)
    (configure-isolated-output-cache output)
    (run-coverage root output)))

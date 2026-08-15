(in-package #:cl-user)

(defparameter *bootstrap-nerima-systems*
  '("cl-boundary-kit"
    "cl-concurrent-kit"
    "cl-dataflow"
    "cl-date-kit"
    "cl-host-kit"
    "cl-observability-kit"
    "cl-prolog"
    "cl-weave"))

(defun %pathname-parent (pathname levels)
  (loop
    with current = pathname
    repeat levels
    do (setf current (uiop:pathname-parent-directory-pathname current))
    finally (return current)))

(defun %adjacent-system-directory (name root)
  (merge-pathnames (format nil "../~A/" name) root))

(defun %ghq-bare-repository-directory (name root)
  (merge-pathnames (format nil "~A.git/" name)
                   (%pathname-parent root 3)))

(defun %git-bare-repository-p (pathname)
  (and (uiop:directory-exists-p pathname)
       (probe-file (merge-pathnames "HEAD" pathname))
       (probe-file (merge-pathnames "objects/" pathname))))

(defun %make-bootstrap-temp-directory ()
  (uiop:ensure-directory-pathname
   (string-right-trim
    '(#\Newline)
    (uiop:run-program '("mktemp" "-d" "-t" "cl-resilience-kit-bootstrap")
                      :output :string))))

(defun %bare-repository-name (repository-directory)
  (let* ((directory-components (pathname-directory repository-directory))
         (leaf (car (last directory-components))))
    (if (uiop:string-suffix-p leaf ".git")
        (subseq leaf 0 (- (length leaf) 4))
        leaf)))

(defun %materialize-bare-repository (repository-directory destination-root)
  (let* ((repository-name (%bare-repository-name repository-directory))
         (archive-path (merge-pathnames (format nil "~A.tar" repository-name)
                                        destination-root))
         (destination-directory
           (merge-pathnames (format nil "~A/" repository-name) destination-root)))
    (ensure-directories-exist destination-directory)
    (uiop:run-program
     (list "git"
           (format nil "--git-dir=~A"
                   (native-namestring repository-directory))
           "archive"
           "--format=tar"
           (format nil "--output=~A" (native-namestring archive-path))
           "HEAD"))
    (uiop:run-program
     (list "tar"
           "-xf"
           (native-namestring archive-path)
           "-C"
           (native-namestring destination-directory)))
    destination-directory))

(defun %discover-nerima-system-directories (root)
  (let ((bootstrap-root nil))
    (flet ((bootstrap-root ()
             (or bootstrap-root
                 (setf bootstrap-root (%make-bootstrap-temp-directory)))))
      (remove nil
              (mapcar
               (lambda (name)
                 (let ((adjacent-directory (%adjacent-system-directory name root)))
                   (cond
                     ((uiop:directory-exists-p adjacent-directory)
                      adjacent-directory)
                     (t
                      (let ((bare-repository
                              (%ghq-bare-repository-directory name root)))
                        (when (%git-bare-repository-p bare-repository)
                          (%materialize-bare-repository bare-repository
                                                        (bootstrap-root))))))))
               *bootstrap-nerima-systems*)))))

(defun bootstrap-cl-resilience-kit (root)
  "Register the project and adjacent NERIMA systems for a local run.

The Nix checks provide dependencies through their isolated source registry.
This bootstrap additionally discovers the sibling checkouts used by local
development. In ghq bare-clone layouts it materializes sibling repositories
into a temporary source tree, including transitive systems such as
CL-HOST-KIT."
  (require :asdf)
  (let ((directories (%discover-nerima-system-directories root)))
    (asdf:initialize-source-registry
     `(:source-registry
       (:tree ,root)
       ,@(mapcar (lambda (directory) `(:tree ,directory)) directories)
       :inherit-configuration))
    (pushnew root asdf:*central-registry* :test #'equal)
    (dolist (directory directories)
      (pushnew directory asdf:*central-registry* :test #'equal)))
  root)

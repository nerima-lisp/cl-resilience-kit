(in-package #:cl-resilience-kit/test)

(defun run-tests ()
  (let ((events (run (root-suite) :reporter :spec)))
    (unless events
      (error "cl-resilience-kit registered no tests."))
    (unless (results-status events)
      (error "cl-resilience-kit test suite failed."))
    t))

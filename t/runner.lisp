(in-package #:resilience-kit/test)

(defun run-tests ()
    (let* ((suite (root-suite))
         (plan (collect-test-plan suite)))
    (unless (plusp (length plan))
      (error "cl-resilience-kit registered no tests."))
    (let ((events (run suite :reporter :spec :timeout-ms 30000)))
      (format t "~&resilience-kit: ~D tests selected; ~D result events.~%"
              (length plan)
              (length events))
      (unless events
        (error "cl-resilience-kit produced no result events."))
      (unless (results-status events)
        (error "cl-resilience-kit test suite failed."))
      t)))

(in-package #:asdf-user)

(asdf:defsystem "cl-resilience-kit"
  :description "Composable resilience primitives for Common Lisp."
  :long-description "Dependency-neutral retry, deadline, circuit-breaker, bulkhead, and rate-limiter primitives with injectable boundary objects."
  :version "1.0.0"
  :author "Community contributors"
  :maintainer "Community contributors"
  :license "MIT"
  :homepage "https://github.com/nerima-lisp/cl-resilience-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-resilience-kit/issues"
  :source-control (:git "https://github.com/nerima-lisp/cl-resilience-kit.git")
  :depends-on ("cl-boundary-kit" "cl-concurrent-kit")
  :pathname "src"
  :serial t
  :components ((:file "package")
               (:file "conditions")
               (:file "context")
               (:file "deadline")
               (:file "retry")
               (:file "retry-execution")
               (:file "circuit-breaker")
               (:file "bulkhead")
               (:file "rate-limiter")
               (:file "composition"))
  :in-order-to ((test-op (test-op "cl-resilience-kit/test"))))

(asdf:defsystem "cl-resilience-kit/test"
  :description "Tests for cl-resilience-kit."
  :long-description "Deterministic cl-weave tests for retry, deadline, breaker, bulkhead, rate limiter, and composition semantics."
  :version "1.0.0"
  :author "Community contributors"
  :maintainer "Community contributors"
  :license "MIT"
  :homepage "https://github.com/nerima-lisp/cl-resilience-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-resilience-kit/issues"
  :source-control (:git "https://github.com/nerima-lisp/cl-resilience-kit.git")
  :depends-on ("cl-resilience-kit" "cl-weave")
  :pathname "t"
  :serial t
  :components ((:file "package")
               (:file "support")
               (:file "runner")
               (:file "retry-test")
               (:file "deadline-test")
               (:file "breaker-test")
               (:file "limiter-test")
               (:file "composition-test"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call :cl-resilience-kit/test :run-tests)))

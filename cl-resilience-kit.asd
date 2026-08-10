(in-package #:asdf-user)

(asdf:defsystem "cl-resilience-kit"
  :description "Composable resilience primitives for Common Lisp."
  :long-description "Dependency-neutral retry, deadline, circuit-breaker, bulkhead, and rate-limiter primitives with injectable boundary objects."
  :version "2.0.0"
  :author "Community contributors"
  :maintainer "Community contributors"
  :license "MIT"
  :homepage "https://github.com/nerima-lisp/cl-resilience-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-resilience-kit/issues"
  :source-control (:git "https://github.com/nerima-lisp/cl-resilience-kit.git")
  :depends-on ((:version "cl-boundary-kit" "2.3.0")
               (:version "cl-concurrent-kit" "0.6.1")
               (:version "cl-date-kit" "1.0.0"))
  :pathname "src"
  :serial t
  :components ((:file "package")
               (:file "conditions")
               (:file "context")
               (:file "cancellation")
               (:file "events")
               (:file "state-store")
               (:file "data-validation")
               (:file "lease-store")
               (:file "lifecycle")
               (:file "deadline")
               (:file "retry-policy")
               (:file "retry-budget")
               (:file "retry")
               (:file "retry-execution")
               (:file "circuit-breaker")
               (:file "distributed-circuit-breaker-definition")
               (:file "distributed-circuit-breaker-state")
               (:file "distributed-circuit-breaker-transition")
               (:file "distributed-circuit-breaker-execution")
               (:file "distributed-circuit-breaker-api")
               (:file "bulkhead")
               (:file "rate-limiter")
               (:file "composition-core")
               (:file "executor")
               (:file "hedging")
               (:file "coalescing")
               (:file "composition"))
  :in-order-to ((test-op (test-op "cl-resilience-kit/test"))))

(asdf:defsystem "cl-resilience-kit/observability"
  :description "Direct cl-observability-kit metrics for resilience events."
  :long-description "An optional metrics integration that publishes resilience event counts and durations through cl-observability-kit."
  :version "2.0.0"
  :author "Community contributors"
  :maintainer "Community contributors"
  :license "MIT"
  :homepage "https://github.com/nerima-lisp/cl-resilience-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-resilience-kit/issues"
  :source-control (:git "https://github.com/nerima-lisp/cl-resilience-kit.git")
  :depends-on ("cl-resilience-kit"
               (:version "cl-observability-kit" "1.0.0"))
  :pathname "src"
  :serial t
  :components ((:file "observability")))

(asdf:defsystem "cl-resilience-kit/test"
  :description "Tests for cl-resilience-kit."
  :long-description "Deterministic cl-weave tests for retry, deadline, breaker, bulkhead, rate limiter, and composition semantics."
  :version "2.0.0"
  :author "Community contributors"
  :maintainer "Community contributors"
  :license "MIT"
  :homepage "https://github.com/nerima-lisp/cl-resilience-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-resilience-kit/issues"
  :source-control (:git "https://github.com/nerima-lisp/cl-resilience-kit.git")
  :depends-on ("cl-resilience-kit/observability" "cl-weave")
  :pathname "t"
  :serial t
  :components ((:file "package")
               (:file "support")
               (:file "runner")
               (:file "data-validation-test")
               (:file "retry-test")
               (:file "retry-distributed-test")
               (:file "deadline-test")
               (:file "breaker-test")
               (:file "limiter-test")
               (:file "composition-test")
               (:file "contract-test")
               (:file "contract-edge-test")
               (:file "distributed-contract-test")
               (:file "distributed-execution-contract-test")
               (:file "condition-contract-test")
               (:file "cps-test")
               (:file "observability-test"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call :cl-resilience-kit/test :run-tests)))

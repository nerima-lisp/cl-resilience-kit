(in-package #:asdf-user)

(asdf:defsystem "cl-resilience-kit"
  :description "Composable resilience primitives for Common Lisp."
  :long-description "Retry, deadline, circuit-breaker, bulkhead, and rate-limiter primitives built on Nerima Lisp packages and injectable boundary objects."
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
               (:file "numeric-support")
               (:file "conditions-base")
               (:file "conditions-retry")
               (:file "conditions-isolation")
               (:file "conditions-distributed")
               (:file "conditions-composition")
               (:file "context")
               (:file "cancellation")
               (:file "events")
               (:file "state-store")
               (:file "data-validation")
               (:file "lease-store-definition")
               (:file "lease-store-execution")
               (:file "lease-store")
               (:file "lifecycle")
               (:file "deadline")
               (:file "retry-policy")
               (:file "retry-scheduling")
               (:file "retry-budget-definition")
               (:file "retry-budget-execution")
               (:file "retry-execution-boundaries")
               (:file "retry-execution-recovery")
               (:file "retry-execution")
               (:file "retry")
               (:file "circuit-breaker-definition")
               (:file "circuit-breaker-state")
               (:file "circuit-breaker")
               (:file "distributed-circuit-breaker-definition")
               (:file "distributed-circuit-breaker-state")
               (:file "distributed-circuit-breaker-transition")
               (:file "distributed-circuit-breaker-execution")
               (:file "distributed-circuit-breaker-api")
               (:file "bulkhead-definition")
               (:file "bulkhead-admission")
               (:file "bulkhead-execution")
               (:file "bulkhead")
               (:file "rate-limiter-definition")
               (:file "rate-limiter-execution")
               (:file "rate-limiter")
               (:file "composition-core")
               (:file "composition-plan")
               (:file "executor-definition")
               (:file "executor-execution")
               (:file "executor")
               (:file "hedging-execution")
               (:file "hedging")
               (:file "coalescing-definition")
               (:file "coalescing-execution")
               (:file "coalescing")
               (:file "composition-support")
               (:file "composition-runtime-context")
               (:file "composition-runtime-execution")
               (:file "composition-runtime-dispatch")
               (:file "composition")
               (:file "composition-macros"))
  :in-order-to ((test-op (test-op "cl-resilience-kit/all-test"))))

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
               (:version "cl-observability-kit" "0.1.0"))
  :pathname "src"
  :serial t
  :components ((:file "observability")))

(asdf:defsystem "cl-resilience-kit/dataflow"
  :description "Optional cl-dataflow integration for resilient pipeline stages."
  :long-description "An optional dataflow integration that wraps cl-dataflow node handlers with cl-resilience-kit execution boundaries."
  :version "2.0.0"
  :author "Community contributors"
  :maintainer "Community contributors"
  :license "MIT"
  :homepage "https://github.com/nerima-lisp/cl-resilience-kit"
  :bug-tracker "https://github.com/nerima-lisp/cl-resilience-kit/issues"
  :source-control (:git "https://github.com/nerima-lisp/cl-resilience-kit.git")
  :depends-on ("cl-resilience-kit"
               (:version "cl-dataflow" "1.1.1"))
  :pathname "src"
  :serial t
  :components ((:file "dataflow")
               (:file "dataflow-runtime")
               (:file "dataflow-macros")))

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
  :depends-on ("cl-resilience-kit"
               (:version "cl-weave" "1.3.0"))
  :pathname "t"
  :serial t
  :components ((:file "package")
               (:file "support")
               (:file "runner")
               (:file "data-validation-test")
               (:file "retry-policy-test")
               (:file "retry-test")
               (:file "deadline-test")
               (:file "breaker-test")
               (:file "bulkhead-test")
               (:file "rate-limiter-test")
               (:file "bulkhead-admission-test")
               (:file "composition-test")
               (:file "composition-context-test")
               (:file "backend-timeout-test")
               (:file "contract-test")
               (:file "contract-edge-test")
               (:file "distributed-contract-test")
               (:file "distributed-execution-contract-test")
               (:file "condition-contract-test")
               (:file "cps-test"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call :cl-resilience-kit/test :run-tests)))

(asdf:defsystem "cl-resilience-kit/observability-test"
  :description "Optional cl-observability-kit integration tests."
  :version "2.0.0"
  :depends-on ("cl-resilience-kit/test"
               "cl-resilience-kit/observability")
  :pathname "t"
  :serial t
  :components ((:file "observability-package")
               (:file "observability-test"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call :cl-resilience-kit/test :run-tests)))

(asdf:defsystem "cl-resilience-kit/dataflow-test"
  :description "Optional cl-dataflow integration tests."
  :version "2.0.0"
  :depends-on ("cl-resilience-kit/test"
               "cl-resilience-kit/dataflow")
  :pathname "t"
  :serial t
  :components ((:file "dataflow-package")
               (:file "dataflow-macro-test")
               (:file "dataflow-test"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call :cl-resilience-kit/test :run-tests)))

(asdf:defsystem "cl-resilience-kit/all-test"
  :description "Full test suite for cl-resilience-kit, including optional integrations."
  :version "2.0.0"
  :depends-on ("cl-resilience-kit/test"
               "cl-resilience-kit/observability-test"
               "cl-resilience-kit/dataflow-test")
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call :cl-resilience-kit/test :run-tests)))

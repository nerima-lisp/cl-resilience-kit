(defpackage #:cl-resilience-kit/observability-test
  (:use #:cl)
  (:shadowing-import-from #:cl-weave #:describe)
  (:import-from #:cl-weave
                #:expect
                #:it
                #:it-each)
  (:import-from #:cl-resilience-kit/test
                #:expect-condition)
  (:import-from #:cl-resilience-kit
                #:call-with-resilience
                #:make-resilience-event
                #:with-resilience-event-handler)
  (:import-from #:cl-resilience-kit/observability
                #:make-resilience-observability
                #:record-resilience-event
                #:resilience-observability-handler
                #:resilience-observability-registry
                #:with-resilience-observability))

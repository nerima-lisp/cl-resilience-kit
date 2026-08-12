(defpackage #:resilience-observability-test
  (:nicknames #:cl-resilience-kit/observability-test)
  (:use #:cl)
  (:shadowing-import-from #:cl-weave #:describe)
  (:import-from #:cl-weave
                #:expect
                #:it
                #:gen-member
                #:it-each
                #:it-fuzz)
  (:import-from #:resilience-kit/test
                #:expect-condition)
  (:import-from #:resilience-kit
                #:call-with-resilience
                #:make-resilience-event
                #:with-resilience-event-handler)
  (:import-from #:resilience-observability
                #:make-resilience-observability
                #:record-resilience-event
                #:resilience-observability-handler
                #:resilience-observability-registry
                #:with-resilience-observability))

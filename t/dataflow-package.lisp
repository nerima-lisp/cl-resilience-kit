(defpackage #:resilience-dataflow-test
  (:nicknames #:cl-resilience-kit/dataflow-test)
  (:use #:cl)
  (:shadowing-import-from #:cl-weave #:describe)
  (:import-from #:cl-weave
                #:expect
                #:gen-integer
                #:it
                #:it-fuzz
                #:with-soft-assertions)
  (:import-from #:resilience-kit/test
                #:expect-macroexpansion-error
                #:expect-pipeline-result)
  (:import-from #:resilience-kit
                #:make-retry-policy)
  (:import-from #:resilience-dataflow
                #:define-pipeline
                #:make-node
                #:make-pipeline
                #:pipeline
                #:pipeline->node
                #:define-resilience-pipeline
                #:make-resilience-node
                #:make-resilience-pipeline
                #:make-resilience-pipeline-node
                #:run-pipeline
                #:run-resilience-pipeline)
  (:import-from #:cl-dataflow
                #:context-metadata
                #:make-context
                #:node
                #:node-handler
                #:node-inputs
                #:node-metadata
                #:node-name
                #:node-outputs
                #:pipeline-metadata
                #:pipeline-stages))

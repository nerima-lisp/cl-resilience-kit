(defpackage #:resilience-dataflow
  (:nicknames #:cl-resilience-kit/dataflow)
  (:use #:cl)
  (:import-from #:resilience-kit
                #:call-with-resilience)
  (:import-from #:cl-dataflow-kit
                #:define-pipeline
                #:make-node
                #:make-pipeline
                #:pipeline
                #:pipeline->node
                #:run-pipeline)
  (:export #:define-pipeline
           #:make-node
           #:make-pipeline
           #:pipeline
           #:pipeline->node
           #:run-pipeline
           #:define-resilience-pipeline
           #:make-resilience-node
           #:make-resilience-pipeline
           #:make-resilience-pipeline-node
           #:run-resilience-pipeline))

(in-package #:resilience-dataflow)

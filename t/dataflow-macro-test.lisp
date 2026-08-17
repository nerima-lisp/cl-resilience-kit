(in-package #:resilience-dataflow-test)

(describe "cl-dataflow-kit macro integration"
  (it "builds a resilience pipeline with cl-dataflow-kit syntax"
    (expect-pipeline-result
     ((define-resilience-pipeline ()
        (:node "double"
         :handler (make-unary-operation (lambda (input) (* input 2)))
         :resilience-options '(:operation :double)))
      :input 21)
     42))

  (it "preserves node shape through define-resilience-pipeline"
    (let* ((pipeline
             (define-resilience-pipeline
                 (:metadata '(:flow :integration))
               (:node "annotated"
                :inputs '(:request)
                :outputs '(:response)
                :metadata '(:kind :test)
                :handler (make-unary-operation #'identity)
                :resilience-options '(:operation :annotated))))
           (node (first (pipeline-stages pipeline))))
      (with-soft-assertions
        (expect (node-name node)
                :to-be
                "annotated")
        (expect (node-inputs node)
                :to-equal
                '("REQUEST"))
        (expect (node-outputs node)
                :to-equal
                '("RESPONSE"))
        (expect (node-metadata node)
                :to-equal
                '(:kind :test))
        (expect (pipeline-metadata pipeline)
                :to-equal
                '(:flow :integration)))))

  (it "rejects node clauses without a handler"
    (expect-macroexpansion-error
     (define-resilience-pipeline ()
       (:node "broken"
        :resilience-options '(:operation :broken)))
     ":handler"))

  (it "keeps node-local resilience options inside define-resilience-pipeline"
    (let* ((attempts 0)
           (policy
             (make-always-retry-policy :max-attempts 2))
           (pipeline
             (define-resilience-pipeline ()
               (:node "increment"
                :handler
                (make-unary-operation
                 (lambda (input)
                   (incf attempts)
                   (if (= attempts 1)
                       (error "retry once")
                       (1+ input))))
                :resilience-options
                (list :operation :increment
                      :retry-policy policy)))))
      (expect-pipeline-result (pipeline :input 9) 10)
      (expect attempts :to-be 2)))

  (it "applies top-level resilience options to every node"
    (let* ((attempts 0)
           (policy
             (make-always-retry-policy :max-attempts 2))
           (pipeline
             (define-resilience-pipeline
                 (:resilience-options
                  (list :operation :increment
                        :retry-policy policy))
               (:node "increment"
                :handler
                (make-unary-operation
                 (lambda (input)
                   (incf attempts)
                   (if (= attempts 1)
                       (error "retry once")
                       (1+ input))))))))
      (expect-pipeline-result (pipeline :input 9) 10)
      (expect attempts :to-be 2)))

  (it "rejects unsupported top-level resilience alias"
    (expect-macroexpansion-error
     (define-resilience-pipeline
         (:resilience '(:operation :top))
       (:node "broken"
        :handler (make-unary-operation #'identity)))
     ":RESILIENCE"))

  (it "rejects unsupported node-local resilience alias"
    (expect-macroexpansion-error
     (define-resilience-pipeline ()
       (:node "broken"
        :handler (make-unary-operation #'identity)
        :resilience '(:operation :node)))
     ":RESILIENCE"))

  (it "lets node-local resilience options override top-level defaults"
    (let* ((attempts 0)
           (default-policy
             (make-always-retry-policy :max-attempts 1))
           (node-policy
             (make-always-retry-policy :max-attempts 2))
           (pipeline
             (define-resilience-pipeline
                 (:resilience-options
                  (list :operation :default
                        :retry-policy default-policy))
               (:node "increment"
                :handler
                (make-unary-operation
                 (lambda (input)
                   (incf attempts)
                   (if (= attempts 1)
                       (error "retry once")
                       (1+ input))))
                :resilience-options
                (list :operation :increment
                      :retry-policy node-policy)))))
      (expect-pipeline-result (pipeline :input 9) 10)
      (expect attempts :to-be 2)))

  (it-fuzz "applies top-level resilience defaults across generated inputs"
      ((value (gen-integer :min -8 :max 8)))
      (:trials 10 :timeout-per-trial 1)
    (let ((pipeline
            (define-resilience-pipeline
                (:resilience-options
                 (list :operation :generated-increment))
              (:node "increment"
               :handler (make-unary-operation #'1+)))))
      (expect-pipeline-result (pipeline :input value)
                              (1+ value)))))

(in-package #:resilience-dataflow-test)

(defun make-unary-operation (operator)
  (check-type operator function)
  (lambda (input context)
    (declare (ignore context))
    (funcall operator input)))

(defun make-contextual-operation (operator)
  (check-type operator function)
  (lambda (input context)
    (funcall operator input context)))

(defun make-always-retry-policy (&key (max-attempts 3))
  (make-retry-policy
   :max-attempts max-attempts
   :retry-safe-p t
   :condition-classifier
   (lambda (condition attempt)
     (declare (ignore condition attempt))
     t)))

(describe "cl-dataflow integration"
  (it "exports the nerima-lisp package nickname"
    (let ((package (find-package "RESILIENCE-DATAFLOW")))
      (expect package :to-be-truthy)
      (when package
        (expect (member "RESILIENCE-DATAFLOW"
                        (package-nicknames package)
                        :test #'string=)
                :to-be-truthy)
        (expect (find-symbol "DEFINE-PIPELINE" package)
                :to-be-truthy)
        (expect (find-symbol "MAKE-NODE" package)
                :to-be-truthy)
        (expect (find-symbol "MAKE-PIPELINE" package)
                :to-be-truthy)
        (expect (find-symbol "PIPELINE" package)
                :to-be-truthy)
        (expect (find-symbol "PIPELINE->NODE" package)
                :to-be-truthy)
        (expect (find-symbol "RUN-PIPELINE" package)
                :to-be-truthy)
        (expect (find-symbol "DEFINE-RESILIENCE-PIPELINE" package)
                :to-be-truthy))))

  (it "re-exports direct cl-dataflow entry points"
    (with-soft-assertions
      (expect (eq (find-symbol "DEFINE-PIPELINE" "RESILIENCE-DATAFLOW")
                  (find-symbol "DEFINE-PIPELINE" "CL-DATAFLOW"))
              :to-be-truthy)
      (expect (eq (nth-value 1
                             (find-symbol "DEFINE-PIPELINE"
                                          "RESILIENCE-DATAFLOW"))
                  :external)
              :to-be-truthy)
      (expect (fboundp 'make-node)
              :to-be-truthy)
      (expect (fboundp 'make-pipeline)
              :to-be-truthy)
      (expect (find-class 'pipeline nil)
              :to-be-truthy)
      (expect (fboundp 'pipeline->node)
              :to-be-truthy)
      (expect (fboundp 'run-pipeline)
              :to-be-truthy)))

  (it "runs a single-stage resilience pipeline"
    (expect-pipeline-result
     ((make-resilience-pipeline
       :name "double"
       :operation (make-unary-operation (lambda (input) (* input 2)))
       :resilience-options '(:operation :double))
      :input 21)
     42))

  (it "retries through the wrapped handler"
    (let* ((attempts 0)
           (policy
             (make-always-retry-policy :max-attempts 3))
           (pipeline
             (make-resilience-pipeline
              :operation (make-unary-operation
                          (lambda (input)
                            (incf attempts)
                            (if (< attempts 3)
                                (error "transient failure")
                                (1+ input))))
              :resilience-options
              (list :operation :increment
                    :retry-policy policy))))
      (expect-pipeline-result (pipeline :input 4) 5)
      (expect attempts :to-be 3)))

  (it "copies resilience options when the pipeline is built"
    (let* ((attempts 0)
           (policy
             (make-always-retry-policy :max-attempts 2))
           (options
             (list :operation :increment
                   :retry-policy policy))
           (pipeline
             (make-resilience-pipeline
              :operation (make-unary-operation
                          (lambda (input)
                            (incf attempts)
                            (if (= attempts 1)
                                (error "retry once")
                                (1+ input))))
              :resilience-options options)))
      (setf (getf options :retry-policy) nil)
      (expect-pipeline-result (pipeline :input 9) 10)
      (expect attempts :to-be 2)))

  (it "creates embeddable nodes and pipeline nodes"
    (let* ((node
             (make-resilience-node
              :name "increment"
              :operation (make-unary-operation #'1+)
              :resilience-options '(:operation :increment)))
           (pipeline-node
             (make-resilience-pipeline-node
              :name "triple"
              :operation (make-unary-operation
                          (lambda (input) (* input 3)))
              :resilience-options '(:operation :triple))))
      (with-soft-assertions
        (expect (node-name node)
                :to-be
                "increment")
        (expect (funcall (node-handler node) 7 nil)
                :to-be
                8)
        (expect (typep pipeline-node 'node)
                :to-be-truthy))))

  (it "forwards cl-dataflow context into the wrapped operation"
    (let ((pipeline
            (make-resilience-pipeline
             :name "capture-context"
             :operation
             (make-contextual-operation
              (lambda (input context)
                (list input
                      (getf (context-metadata context)
                            :request-id))))
             :resilience-options '(:operation :capture-context)))
          (context
            (make-context
             :metadata '(:request-id "req-42"))))
      (expect-pipeline-result
       (pipeline :input 21 :context context)
       '(21 "req-42"))))

  (it "preserves cl-dataflow node interface fields"
    (let ((node
            (make-resilience-node
             :name "annotated"
             :inputs '(:request)
             :outputs '(:response)
             :metadata '(:kind :test)
             :operation (make-unary-operation #'identity)
             :resilience-options '(:operation :annotated))))
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
                '(:kind :test)))))

  (it "preserves pipeline metadata"
    (let ((pipeline
            (make-resilience-pipeline
             :name "annotated-pipeline"
             :metadata '(:owner :resilience-kit)
             :operation (make-unary-operation #'identity)
             :resilience-options '(:operation :annotated-pipeline))))
      (expect (pipeline-metadata pipeline)
              :to-equal
              '(:owner :resilience-kit))))

  (it "runs pipelines through run-resilience-pipeline"
    (let ((pipeline
            (make-resilience-pipeline
             :name "runner"
             :operation (make-unary-operation (lambda (input) (* input 4)))
             :resilience-options '(:operation :runner))))
      (expect (run-resilience-pipeline pipeline :input 6)
              :to-be
              24)))

  (it-fuzz "keeps cl-dataflow pipeline semantics across generated inputs"
      ((value (gen-integer :min -8 :max 8)))
      (:trials 10 :timeout-per-trial 1)
    (let ((pipeline
            (make-resilience-pipeline
             :name "generated-increment"
             :operation (make-unary-operation #'1+)
             :resilience-options '(:operation :generated-increment))))
      (expect (run-resilience-pipeline pipeline :input value)
              :to-be
              (1+ value))))
  )

(in-package #:resilience-dataflow)

(defun %merge-resilience-options (defaults overrides)
  "Merge two resilience-options plists into one plist with each key
appearing at most once. OVERRIDES wins over DEFAULTS for a shared key;
call-with-resilience rejects a plist with a repeated keyword option."
  (cond ((null defaults)
         overrides)
        ((null overrides)
         defaults)
        (t
         (let ((missing (gensym "MISSING-OPTION")))
           (append overrides
                   (loop for (key value) on defaults by #'cddr
                         when (eq (getf overrides key missing) missing)
                           append (list key value)))))))

(defun %copy-resilience-options (resilience-options)
  (copy-list resilience-options))

(defun %call-resilience-operation
    (operation input context resilience-options)
  (apply #'call-with-resilience
         (lambda ()
           (funcall operation input context))
         resilience-options))

(defun %resilience-handler (operation resilience-options)
  (check-type operation function)
  (let ((options (%copy-resilience-options resilience-options)))
    (lambda (input context)
      (%call-resilience-operation
       operation
       input
       context
       options))))

(defun %make-resilience-stage
    (name operation inputs outputs metadata resilience-options)
  (make-resilience-node
   :name name
   :operation operation
   :inputs inputs
   :outputs outputs
   :metadata metadata
   :resilience-options resilience-options))

(defun make-resilience-node
    (&key
       (name "resilience")
       operation
       inputs
       outputs
       metadata
       (resilience-options nil))
  "Return a cl-dataflow node whose handler runs OPERATION through call-with-resilience."
  (check-type operation function)
  (make-node name
             :inputs inputs
             :outputs outputs
             :metadata metadata
             :handler (%resilience-handler operation resilience-options)))

(defun make-resilience-pipeline
    (&key
       (name "resilience")
       operation
       metadata
       inputs
       outputs
       (resilience-options nil))
  "Return a one-stage cl-dataflow pipeline around OPERATION."
  (check-type operation function)
  (make-pipeline
   :stages
   (list (%make-resilience-stage
          name
          operation
          inputs
          outputs
          metadata
          resilience-options))
   :metadata metadata))

(defun make-resilience-pipeline-node
    (&key
       (name "resilience")
       operation
       metadata
       inputs
       outputs
       (resilience-options nil))
  "Wrap a resilience pipeline as a reusable cl-dataflow node."
  (check-type operation function)
  (let ((pipeline
          (make-resilience-pipeline
           :name name
           :operation operation
           :metadata metadata
           :inputs inputs
           :outputs outputs
           :resilience-options resilience-options)))
    (pipeline->node pipeline name :metadata metadata)))

(defun run-resilience-pipeline (pipeline &key input context parallel)
  "Run PIPELINE through cl-dataflow and return its result."
  (check-type pipeline pipeline)
  (run-pipeline pipeline
                :input input
                :context context
                :parallel parallel))

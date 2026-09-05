(in-package #:resilience-kit/test)

(defconstant +test-monotonic-units-per-second+ 1)

(defstruct (test-fixture
            (:constructor %make-test-fixture
                (clock random-source sleeper sleeps)))
  clock
  random-source
  sleeper
  sleeps)

(defun make-test-fixture (&key (start 0) (random-values '(500000)))
  "Return fake boundary objects and a sleeper that advances the fake clock."
  (let* ((clock (make-fake-clock
                 :start start
                 :monotonic-start start))
         (fixture (%make-test-fixture
                   clock
                   (make-test-random-source
                    :values random-values)
                   nil
                   nil)))
    (setf (test-fixture-sleeper fixture)
          (make-sleeper
           :sleep-fn
           (lambda (seconds)
             (push seconds (test-fixture-sleeps fixture))
             (advance-fake-clock clock seconds))))
    fixture))

(defun fixture-sleeps (fixture)
  (reverse (test-fixture-sleeps fixture)))

(defun advance-fixture (fixture seconds)
  (advance-fake-clock
   (test-fixture-clock fixture)
   seconds))

(defun approximately-equal-p (left right &optional (epsilon 1d-9))
  (<= (abs (- (coerce left 'double-float)
              (coerce right 'double-float)))
      epsilon))

(defun join-all (threads &key (timeout 5))
  "Join THREADS, waiting at most TIMEOUT seconds for each thread."
  (mapcar (lambda (thread)
            (join-thread thread :timeout timeout))
          threads))

(defmacro expect-condition (thunk condition-type)
  (let ((caught (gensym "CAUGHT-"))
        (normalized-condition-type
          (if (and (consp condition-type)
                   (eq (car condition-type) 'quote)
                   (consp (cdr condition-type))
                   (null (cddr condition-type)))
              (cadr condition-type)
              condition-type)))
    `(let ((,caught nil))
       (handler-case
           (progn
             (funcall ,thunk)
             (error "expected thunk to signal ~S but it returned normally"
                    ',condition-type))
         (,normalized-condition-type (condition)
           (setf ,caught condition)))
       ,caught)))

(defmacro expect-condition-form (form condition-type)
  `(expect-condition
    (lambda () ,form)
    ,condition-type))

(defmacro expect-error-forms (&body forms)
  `(progn
     ,@(mapcar (lambda (form)
                 `(expect-condition-form ,form 'error))
               forms)))

(defmacro expect-invalid-calls ((callee &optional (condition-type ''error))
                                &body argument-lists)
  `(progn
     ,@(mapcar (lambda (arguments)
                 `(expect-condition-form
                   (apply ,callee ',arguments)
                   ,condition-type))
               argument-lists)))

(defmacro expect-invalid-macroexpansions
    ((&optional (condition-type ''error)) &body forms)
  `(progn
     ,@(mapcar (lambda (form)
                 `(expect-condition-form
                   (macroexpand-1 ',form)
                   ,condition-type))
               forms)))

(defmacro expect-macroexpansions (&body cases)
  `(progn
     ,@(mapcar
        (lambda (case)
          (destructuring-bind (form expected) case
            `(expect (macroexpand-1 ',form)
                     :to-equal
                     ',expected)))
        cases)))

(defmacro expect-macroexpansion-error (form expected-fragment)
  (let ((condition-variable (gensym "CONDITION-")))
    `(let ((,condition-variable
             (handler-case
                 (progn
                   (macroexpand-1 ',form)
                   nil)
               (error (caught-condition)
                 caught-condition))))
       (expect ,condition-variable :to-be-truthy)
       (expect (princ-to-string ,condition-variable)
               :to-contain
               ,expected-fragment))))

(defmacro expect-pipeline-result
    ((pipeline-form &key input context parallel) expected)
  `(expect (%run-dataflow-pipeline
            ,pipeline-form
            :input ,input
            :context ,context
            :parallel ,parallel)
           :to-equal
           ,expected))

(defmacro expect-retry-exhausted-form (form)
  `(expect-condition-form ,form 'retry-exhausted))

(defmacro expect-test-retry-exhausted
    ((fixture policy thunk &rest args))
  `(expect-retry-exhausted-form
    (call-with-test-retry ,fixture ,policy ,thunk ,@args)))

(defun %run-dataflow-pipeline (pipeline-form &key input context parallel)
  (let* ((package (or (find-package "RESILIENCE-DATAFLOW")
                      (error "RESILIENCE-DATAFLOW package is not available")))
         (symbol (or (find-symbol "RUN-RESILIENCE-PIPELINE" package)
                     (error "RUN-RESILIENCE-PIPELINE is not exported from RESILIENCE-DATAFLOW"))))
    (funcall symbol
             pipeline-form
             :input input
             :context context
             :parallel parallel)))

(defun expect-condition-contracts (specifications)
  (dolist (specification specifications)
    (destructuring-bind (type initargs &rest fields) specification
      (let ((condition (apply #'make-condition type initargs)))
        (expect (typep condition type) :to-be-truthy)
        (with-soft-assertions
          (dolist (field fields)
            (destructuring-bind (reader expected) field
              (expect (funcall reader condition) :to-be expected))))))))

(defun make-blocking-operation
    (entered release &key (timeout 5) (result :held))
  (lambda ()
    (signal-semaphore entered)
    (wait-on-semaphore release :timeout timeout)
    result))

(defun call-with-test-retry (fixture policy thunk &rest args)
  (apply #'call-with-retry
         policy
         thunk
         :clock (test-fixture-clock fixture)
         :monotonic-units-per-second +test-monotonic-units-per-second+
         :sleeper (test-fixture-sleeper fixture)
         args))

(defun always-retry-condition (condition attempt)
  (declare (ignore condition attempt))
  t)

(defun make-test-rate-limiter
    (fixture
     &key (capacity 1)
       (refill-rate 1)
       (initial-tokens nil initial-tokens-p)
       (clock (test-fixture-clock fixture))
       (monotonic-units-per-second +test-monotonic-units-per-second+)
       (sleeper (test-fixture-sleeper fixture)))
  (apply #'make-rate-limiter
         :capacity capacity
         :refill-rate refill-rate
         :clock clock
         :monotonic-units-per-second monotonic-units-per-second
         :sleeper sleeper
         (when initial-tokens-p
           (list :initial-tokens initial-tokens))))

(defmacro with-test-fixture ((fixture &rest options) &body body)
  `(let ((,fixture (make-test-fixture ,@options)))
     ,@body))

(defmacro with-test-retry-policy
    ((fixture policy &rest policy-options) &body body)
  `(with-test-fixture (,fixture)
     (let ((,policy (make-retry-policy ,@policy-options)))
       ,@body)))

(defmacro with-test-condition-retry-policy
    ((fixture policy &rest policy-options) &body body)
  `(with-test-retry-policy
       (,fixture
        ,policy
        :retry-safe-p t
        :condition-classifier #'always-retry-condition
        ,@policy-options)
     ,@body))

(defmacro with-test-rate-limiter ((fixture limiter &rest options) &body body)
  `(with-test-fixture (,fixture)
     (let ((,limiter (make-test-rate-limiter ,fixture ,@options)))
       ,@body)))

(defmacro with-blocking-operation-thread
    ((entered release &key (timeout 5) (result :held)) invocation &body body)
  (let ((thread (gensym "THREAD-")))
    `(let* ((,entered (make-semaphore :count 0))
            (,release (make-semaphore :count 0))
            (,thread (make-thread (lambda () ,invocation))))
       (unwind-protect
            (progn
              (expect (wait-on-semaphore ,entered :timeout ,timeout)
                      :to-be-truthy)
              ,@body)
         (signal-semaphore ,release)
         (expect (join-thread ,thread :timeout ,timeout) :to-be ,result)))))

(defmacro with-held-queued-bulkhead
    ((bulkhead entered release &key (timeout 5) (result :held)) &body body)
  `(with-blocking-operation-thread
       (,entered ,release :timeout ,timeout :result ,result)
       (resilience-kit:queued-bulkhead-call
        ,bulkhead
        (make-blocking-operation
         ,entered
         ,release
         :timeout ,timeout
         :result ,result))
     ,@body))

(defmacro with-test-executor ((executor &rest options) &body body)
  `(let ((,executor (make-resilience-executor ,@options)))
     (unwind-protect
          (progn ,@body)
       (unless (resilience-executor-shutdown-p ,executor)
         (resilience-executor-shutdown ,executor :wait t)))))

(defmacro with-test-lease-store
    ((fixture store &rest options) &body body)
  `(let* ((,fixture (make-test-fixture))
          (,store (make-memory-lease-store
                   :clock (test-fixture-clock ,fixture)
                   :monotonic-units-per-second
                   +test-monotonic-units-per-second+
                   ,@options)))
     ,@body))

(defmacro with-captured-resilience-dispatch
    ((success-values error-condition) invocation &body body)
  (let ((capture-success (gensym "CAPTURE-SUCCESS-"))
        (capture-error (gensym "CAPTURE-ERROR-")))
    `(let ((,success-values nil)
           (,error-condition nil))
       (flet ((,capture-success (&rest values)
                (setf ,success-values values))
              (,capture-error (condition)
                (setf ,error-condition condition)))
         ,(subst capture-error
                 '%capture-error
                 (subst capture-success '%capture-success invocation)))
       ,@body)))

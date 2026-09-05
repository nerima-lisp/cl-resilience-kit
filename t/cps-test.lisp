(in-package #:resilience-kit/test)

(describe "continuation-oriented test contracts"
  (around-each (next)
    (with-continuation-values (values continuation continuation-called-p)
        ((lambda (continuation)
           (funcall continuation (funcall next) :hook-complete))
         #'continuation)
      (expect continuation-called-p :to-be-truthy)
      (expect (second values) :to-be :hook-complete)
      (first values)))

  (it "preserves all values through a continuation"
    (expect-assertions 3)
    (with-continuation-values (values continuation continuation-called-p)
        ((lambda (continuation)
           (funcall continuation :accepted 42))
         #'continuation)
      (expect continuation-called-p :to-be-truthy)
      (expect (first values) :to-be :accepted)
      (expect (second values) :to-be 42)))

  (it "preserves all values through the resilience composition boundary"
    (expect-assertions 2)
    (multiple-value-bind (first second)
        (call-with-resilience
         (lambda () (values :accepted 42)))
      (expect first :to-be :accepted)
      (expect second :to-be 42)))

  (it "dispatches all values through the public continuation boundary"
    (expect-assertions 2)
    (with-captured-resilience-dispatch
        (success-values error-condition)
        (call-with-resilience/k
         (lambda () (values :accepted 42))
         #'%capture-success
         #'%capture-error)
      (expect success-values :to-equal '(:accepted 42))
      (expect error-condition :to-be nil)))

  (it "dispatches operation errors to the error continuation"
    (expect-assertions 2)
    (with-captured-resilience-dispatch
        (success-values error-condition)
        (call-with-resilience/k
         (lambda ()
           (error 'simple-error :format-control "operation failure"))
         #'%capture-success
         #'%capture-error)
      (expect success-values :to-be nil)
      (expect (typep error-condition 'simple-error) :to-be-truthy)))

  (it "supports the block and continuation macros"
    (expect-assertions 3)
    (expect
     (resilience-kit:with-resilience
         (:operation :macro)
       (values :accepted 42))
     :to-be
     :accepted)
    (with-captured-resilience-dispatch
        (success-values error-condition)
        (resilience-kit:with-resilience/k
            (#'%capture-success
             #'%capture-error
             :operation :macro)
          (values :accepted 42))
      (expect success-values :to-equal '(:accepted 42))
      (expect error-condition :to-be nil)))

  (it "rejects malformed static options"
    (expect-assertions 0)
    (dolist (form
              '((resilience-kit:with-resilience (:unknown t) :ok)
                (resilience-kit:with-resilience (:operation) :ok)
                (resilience-kit:with-resilience (operation :macro) :ok)
                (resilience-kit:with-resilience
                    (:operation :first :operation :second)
                  :ok)
                (resilience-kit:with-resilience/k
                    ((lambda (&rest values)
                       (declare (ignore values)))
                     (lambda (condition)
                       (declare (ignore condition)))
                     :operation :first
                     :operation :second)
                  :ok)))
      (expect-condition
       (lambda () (macroexpand-1 form))
       'error)))

  (it "rejects malformed dynamic option plists"
    (expect-assertions 0)
    (expect-condition
     (lambda ()
       (call-with-resilience (lambda () :ok) :unknown t))
     'error)
    (expect-condition
     (lambda ()
       (call-with-resilience (lambda () :ok) :operation))
     'error))

  (it "accepts dynamically composed option plists"
    (expect-assertions 1)
    (let ((options (list :operation :dynamic)))
      (expect (apply #'call-with-resilience (lambda () :accepted) options)
              :to-be :accepted)))

  (it "does not route continuation errors through the error continuation"
    (expect-assertions 1)
    (let ((error-continuation-called-p nil))
      (expect-condition
       (lambda ()
         (call-with-resilience/k
          (lambda () :accepted)
          (lambda (&rest values)
            (declare (ignore values))
            (error 'simple-error :format-control "continuation failure"))
          (lambda (condition)
            (declare (ignore condition))
            (setf error-continuation-called-p t))))
       'simple-error)
      (expect error-continuation-called-p :to-be nil))))

(defun test-weave-hook ()
  :original)

(describe "nerima-lisp test contracts"
  (it "polls a matcher without a sleep-based test helper"
    (let ((attempts 0))
      (expect-poll (lambda () (incf attempts))
                   (:timeout-ms 100 :interval-ms 0)
                   :to-be 3)
      (expect attempts :to-be 3)))

  (it "restores mocked boundary functions"
    (expect (funcall (symbol-function 'test-weave-hook)) :to-be :original)
    (with-mocked-functions (((symbol-function 'test-weave-hook)
                             (lambda () :mocked)))
      (expect (funcall (symbol-function 'test-weave-hook)) :to-be :mocked))
    (expect (funcall (symbol-function 'test-weave-hook)) :to-be :original))

  (it-fuzz "runs resilience input contracts through generated values"
      ((value (gen-integer :min -4 :max 4)))
      (:trials 8 :timeout-per-trial 1)
    (multiple-value-bind (tag result)
        (call-with-resilience
         (lambda () (values :accepted (1+ value))))
      (expect tag :to-be :accepted)
      (expect result :to-be (1+ value)))))

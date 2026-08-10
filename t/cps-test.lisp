(in-package #:cl-resilience-kit/test)

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
    (with-continuation-values (values continuation continuation-called-p)
        ((lambda (continuation)
           (funcall continuation :accepted 42))
         #'continuation)
      (expect continuation-called-p :to-be-truthy)
      (expect (first values) :to-be :accepted)
      (expect (second values) :to-be 42)))

  (it "preserves all values through the resilience composition boundary"
    (multiple-value-bind (first second)
        (call-with-resilience
         (lambda () (values :accepted 42)))
      (expect first :to-be :accepted)
      (expect second :to-be 42)))

  (it "dispatches all values through the public continuation boundary"
    (let ((success-values nil)
          (error-called-p nil))
      (call-with-resilience/k
       (lambda () (values :accepted 42))
       (lambda (&rest values)
         (setf success-values values))
       (lambda (condition)
         (declare (ignore condition))
         (setf error-called-p t)))
      (expect success-values :to-equal '(:accepted 42))
      (expect error-called-p :to-be nil)))

  (it "dispatches operation errors to the error continuation"
    (let ((success-called-p nil)
          (error-condition nil))
      (call-with-resilience/k
       (lambda ()
         (error 'simple-error :format-control "operation failure"))
       (lambda (&rest values)
         (declare (ignore values))
         (setf success-called-p t))
       (lambda (condition)
         (setf error-condition condition)))
      (expect success-called-p :to-be nil)
      (expect (typep error-condition 'simple-error) :to-be-truthy)))

  (it "supports the block and continuation macros"
    (expect
     (cl-resilience-kit:with-resilience
         (:operation :macro)
       (values :accepted 42))
     :to-be
     :accepted)
    (let ((success-values nil)
          (error-called-p nil))
      (cl-resilience-kit:with-resilience/k
          ((lambda (&rest values)
             (setf success-values values))
           (lambda (condition)
             (declare (ignore condition))
             (setf error-called-p t))
           :operation :macro)
        (values :accepted 42))
      (expect success-values :to-equal '(:accepted 42))
      (expect error-called-p :to-be nil)))

  (it "rejects unknown and duplicate static options"
    (dolist (form
              '((cl-resilience-kit:with-resilience (:unknown t) :ok)
                (cl-resilience-kit:with-resilience
                    (:operation :first :operation :second)
                  :ok)
                (cl-resilience-kit:with-resilience/k
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

  (it "does not route continuation errors through the error continuation"
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

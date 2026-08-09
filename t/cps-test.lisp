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

  (it "does not route continuation errors through the error continuation"
    (let ((error-continuation-called-p nil))
      (expect-condition
       (lambda ()
         (cl-resilience-kit::%call-with-resilience/k
          (lambda () :accepted)
          (lambda (&rest values)
            (declare (ignore values))
            (error 'simple-error :format-control "continuation failure"))
          (lambda (condition)
            (declare (ignore condition))
            (setf error-continuation-called-p t))))
       'simple-error)
      (expect error-continuation-called-p :to-be nil))))

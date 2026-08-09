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
      (expect (second values) :to-be 42))))

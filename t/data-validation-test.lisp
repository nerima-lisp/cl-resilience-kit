(in-package #:cl-resilience-kit/test)

(describe "data validation boundaries"
  (it-property "proper-list accepts generated finite lists"
      ((value (gen-list (gen-integer :min -4 :max 4)
                        :min-length 0
                        :max-length 8)))
    (expect
     (cl-resilience-kit::%proper-list-p value)
     :to-be-truthy))

  (it "accepts finite property lists"
    (expect
     (cl-resilience-kit::%proper-list-p '(:window-start 0))
     :to-be-truthy))

  (it "rejects dotted lists"
    (expect
     (cl-resilience-kit::%proper-list-p '(:window-start . 0))
     :to-be
     nil))

  (it "rejects circular lists"
    (let ((value (list :window-start 0)))
      (setf (cdr value) value)
      (expect
       (cl-resilience-kit::%proper-list-p value)
       :to-be
       nil))))

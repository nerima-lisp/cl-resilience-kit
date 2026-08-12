(in-package #:resilience-kit)

(defmacro with-resilience-lease
    ((lease store key owner &rest options) &body body)
  "Acquire LEASE for BODY and release it on every exit path."
  `(let ((,lease (acquire-resilience-lease ,store ,key ,owner
                                           ,@options)))
     (when ,lease
       (unwind-protect
            (progn ,@body)
         (release-resilience-lease ,lease :ignore-lost-p t)))))

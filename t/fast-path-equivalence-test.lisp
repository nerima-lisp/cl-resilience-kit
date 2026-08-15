(in-package #:resilience-kit/test)

;;; CALL-WITH-RESILIENCE has a compiler macro that, for literal keyword
;;; options known at macroexpansion time, expands directly to one of the
;;; "-direct" dispatch functions instead of going through the runtime
;;; option parser and %CALL-WITH-RESILIENCE-DISPATCH-MODE.  That runtime
;;; dispatcher additionally consults *RESILIENCE-EVENT-HANDLER*, so a call
;;; made through APPLY (or any other non-literal call the compiler macro
;;; cannot see) can select a different dispatch mode than the same options
;;; written literally in source position.  These tests hold the two paths
;;; to the same observable contract: same returned values, same ordered
;;; event-type sequence, and the same condition on failure.

(defun %make-event-log ()
  "Return a fresh mutable box for collecting event types in call order."
  (cons nil nil))

(defun %event-log-handler (log)
  "Return an event handler that records each event's type into LOG."
  (lambda (event)
    (push (resilience-event-type event) (car log))))

(defun %event-log-types (log)
  "Return the event types recorded into LOG, oldest first."
  (reverse (car log)))

(describe "call-with-resilience fast-path/generic-path equivalence"
  (it "matches the generic dispatch for no options under an inherited handler"
    (let ((fast-log (%make-event-log))
          (generic-log (%make-event-log))
          (fast-attempts 0)
          (generic-attempts 0)
          (fast-result nil)
          (generic-result nil))
      (with-resilience-event-handler ((%event-log-handler fast-log))
        (setf fast-result
              (call-with-resilience
               (lambda ()
                 (incf fast-attempts)
                 :fast-ok))))
      (with-resilience-event-handler ((%event-log-handler generic-log))
        (setf generic-result
              (apply #'call-with-resilience
                     (lambda ()
                       (incf generic-attempts)
                       :fast-ok)
                     nil)))
      (expect fast-result :to-be :fast-ok)
      (expect generic-result :to-be :fast-ok)
      (expect fast-attempts :to-be 1)
      (expect generic-attempts :to-be 1)
      (expect (%event-log-types fast-log)
              :to-equal
              (%event-log-types generic-log))))

  (it "matches the generic dispatch for :operation only under an inherited handler"
    (let ((fast-log (%make-event-log))
          (generic-log (%make-event-log))
          (fast-attempts 0)
          (generic-attempts 0)
          (fast-result nil)
          (generic-result nil))
      (with-resilience-event-handler ((%event-log-handler fast-log))
        (setf fast-result
              (call-with-resilience
               (lambda ()
                 (incf fast-attempts)
                 :probe-ok)
               :operation :fast-path-probe)))
      (with-resilience-event-handler ((%event-log-handler generic-log))
        (setf generic-result
              (apply #'call-with-resilience
                     (lambda ()
                       (incf generic-attempts)
                       :probe-ok)
                     (list :operation :fast-path-probe))))
      (expect fast-result :to-be :probe-ok)
      (expect generic-result :to-be :probe-ok)
      (expect fast-attempts :to-be 1)
      (expect generic-attempts :to-be 1)
      (expect (%event-log-types fast-log)
              :to-equal
              (%event-log-types generic-log))))

  (it "matches the generic dispatch for :retry-policy only when a retry succeeds"
    (let ((fast-log (%make-event-log))
          (generic-log (%make-event-log))
          (fast-attempts 0)
          (generic-attempts 0)
          (fast-policy (make-retry-policy
                        :max-attempts 2
                        :initial-delay 0
                        :retry-safe-p t
                        :condition-classifier #'always-retry-condition))
          (generic-policy (make-retry-policy
                           :max-attempts 2
                           :initial-delay 0
                           :retry-safe-p t
                           :condition-classifier #'always-retry-condition))
          (fast-result nil)
          (generic-result nil))
      (with-resilience-event-handler ((%event-log-handler fast-log))
        (setf fast-result
              (call-with-resilience
               (lambda ()
                 (incf fast-attempts)
                 (if (= fast-attempts 1)
                     (error "transient failure")
                     :retried-ok))
               :retry-policy fast-policy)))
      (with-resilience-event-handler ((%event-log-handler generic-log))
        (setf generic-result
              (apply #'call-with-resilience
                     (lambda ()
                       (incf generic-attempts)
                       (if (= generic-attempts 1)
                           (error "transient failure")
                           :retried-ok))
                     (list :retry-policy generic-policy))))
      (expect fast-result :to-be :retried-ok)
      (expect generic-result :to-be :retried-ok)
      (expect fast-attempts :to-be 2)
      (expect generic-attempts :to-be 2)
      (expect (%event-log-types fast-log)
              :to-equal
              (%event-log-types generic-log))))

  (it "matches the generic dispatch for :retry-policy only when every attempt fails"
    (let ((fast-log (%make-event-log))
          (generic-log (%make-event-log))
          (fast-attempts 0)
          (generic-attempts 0)
          (fast-policy (make-retry-policy
                        :max-attempts 2
                        :initial-delay 0
                        :retry-safe-p t
                        :condition-classifier #'always-retry-condition))
          (generic-policy (make-retry-policy
                           :max-attempts 2
                           :initial-delay 0
                           :retry-safe-p t
                           :condition-classifier #'always-retry-condition))
          (fast-condition nil)
          (generic-condition nil))
      (with-resilience-event-handler ((%event-log-handler fast-log))
        (setf fast-condition
              (expect-condition
               (lambda ()
                 (call-with-resilience
                  (lambda ()
                    (incf fast-attempts)
                    (error "persistent failure"))
                  :retry-policy fast-policy))
               'retry-exhausted)))
      (with-resilience-event-handler ((%event-log-handler generic-log))
        (setf generic-condition
              (expect-condition
               (lambda ()
                 (apply #'call-with-resilience
                        (lambda ()
                          (incf generic-attempts)
                          (error "persistent failure"))
                        (list :retry-policy generic-policy)))
               'retry-exhausted)))
      (expect (typep fast-condition 'retry-exhausted) :to-be-truthy)
      (expect (typep generic-condition 'retry-exhausted) :to-be-truthy)
      (expect (retry-exhausted-attempts fast-condition) :to-be 2)
      (expect (retry-exhausted-attempts generic-condition) :to-be 2)
      (expect (typep (retry-exhausted-last-condition fast-condition)
                     'simple-error)
              :to-be-truthy)
      (expect (typep (retry-exhausted-last-condition generic-condition)
                     'simple-error)
              :to-be-truthy)
      (expect fast-attempts :to-be 2)
      (expect generic-attempts :to-be 2)
      (expect (%event-log-types fast-log)
              :to-equal
              (%event-log-types generic-log))))

  (it "matches the generic dispatch for a literal :event-handler option only"
    (let ((fast-log (%make-event-log))
          (generic-log (%make-event-log))
          (fast-attempts 0)
          (generic-attempts 0)
          (fast-result nil)
          (generic-result nil))
      (setf fast-result
            (call-with-resilience
             (lambda ()
               (incf fast-attempts)
               :direct-ok)
             :event-handler (%event-log-handler fast-log)))
      (setf generic-result
            (apply #'call-with-resilience
                   (lambda ()
                     (incf generic-attempts)
                     :direct-ok)
                   (list :event-handler (%event-log-handler generic-log))))
      (expect fast-result :to-be :direct-ok)
      (expect generic-result :to-be :direct-ok)
      (expect fast-attempts :to-be 1)
      (expect generic-attempts :to-be 1)
      (expect (%event-log-types fast-log)
              :to-equal
              (%event-log-types generic-log))
      (expect (car (last (%event-log-types fast-log)))
              :to-be
              :operation-complete)))

  (it "matches the generic dispatch for a literal :event-handler option only, when the thunk fails"
    (let ((fast-log (%make-event-log))
          (generic-log (%make-event-log))
          (fast-attempts 0)
          (generic-attempts 0)
          (fast-condition nil)
          (generic-condition nil))
      (setf fast-condition
            (expect-condition
             (lambda ()
               (call-with-resilience
                (lambda ()
                  (incf fast-attempts)
                  (error "direct failure"))
                :event-handler (%event-log-handler fast-log)))
             'error))
      (setf generic-condition
            (expect-condition
             (lambda ()
               (apply #'call-with-resilience
                      (lambda ()
                        (incf generic-attempts)
                        (error "direct failure"))
                      (list :event-handler (%event-log-handler generic-log))))
             'error))
      (expect (typep fast-condition 'simple-error) :to-be-truthy)
      (expect (typep generic-condition 'simple-error) :to-be-truthy)
      (expect fast-attempts :to-be 1)
      (expect generic-attempts :to-be 1)
      (expect (%event-log-types fast-log)
              :to-equal
              (%event-log-types generic-log))
      (expect (car (last (%event-log-types fast-log)))
              :to-be
              :operation-failed))))

(in-package #:cl-resilience-kit/test)

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
  (let* ((clock (cl-boundary-kit:make-fake-clock
                 :start start
                 :monotonic-start start))
         (fixture (%make-test-fixture
                   clock
                   (cl-boundary-kit:make-test-random-source
                    :values random-values)
                   nil
                   nil)))
    (setf (test-fixture-sleeper fixture)
          (cl-boundary-kit:make-sleeper
           :sleep-fn
           (lambda (seconds)
             (push seconds (test-fixture-sleeps fixture))
             (cl-boundary-kit:advance-fake-clock clock seconds))))
    fixture))

(defun fixture-sleeps (fixture)
  (reverse (test-fixture-sleeps fixture)))

(defun advance-fixture (fixture seconds)
  (cl-boundary-kit:advance-fake-clock
   (test-fixture-clock fixture)
   seconds))

(defun approximately-equal-p (left right &optional (epsilon 1d-9))
  (<= (abs (- (coerce left 'double-float)
              (coerce right 'double-float)))
      epsilon))

(defun join-all (threads)
  (mapcar (lambda (thread)
            (join-thread thread :timeout 5))
          threads))

(defun expect-condition (thunk condition-type)
  (let ((caught nil))
      (handler-case
          (funcall thunk)
      (condition (condition)
        (if (typep condition condition-type)
            (setf caught condition)
            (error condition))))
    caught))

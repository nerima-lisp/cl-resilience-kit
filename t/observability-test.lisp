(in-package #:resilience-observability-test)

(defun %observability-metric (registry name)
  (find name
        (observability-kit:metric-snapshot registry)
        :key #'observability-kit:metric-snapshot-name
        :test #'string=))

(defun %observability-sample (snapshot labels)
  (find labels
        (observability-kit:metric-snapshot-samples snapshot)
        :key #'observability-kit:metric-sample-labels
        :test #'equal))

(defun %expected-observability-label (value)
  (typecase value
    (null "unknown")
    (string (if (string= value "") "unknown" value))
    (symbol (string-downcase (symbol-name value)))
    (t (princ-to-string value))))

(describe "cl-observability-kit integration"
  (it "exports the nerima-lisp package nickname"
    (let ((package (find-package "RESILIENCE-OBSERVABILITY")))
      (expect package :to-be-truthy)
      (when package
        (expect (eq package (find-package "CL-RESILIENCE-KIT/OBSERVABILITY"))
                :to-be-truthy)
        (expect (find-symbol "MAKE-RESILIENCE-OBSERVABILITY" package)
                :to-be-truthy))))

  (it "publishes counters and finite event durations"
    (let* ((observability (make-resilience-observability))
           (event (make-resilience-event
                   :type :operation-complete
                   :operation :read
                   :duration 0.25d0))
           (handler (resilience-observability-handler observability)))
      (expect (funcall handler event) :to-be event)
      (let* ((registry (resilience-observability-registry observability))
             (counter (%observability-metric registry
                                             "resilience_events_total"))
             (histogram
               (%observability-metric registry
                                      "resilience_event_duration_seconds"))
             (labels '(("event_type" . "operation-complete")
                       ("operation" . "read")))
             (counter-sample
               (%observability-sample counter labels))
             (histogram-sample
               (%observability-sample histogram labels)))
        (expect counter :to-be-truthy)
        (expect histogram :to-be-truthy)
        (when (and counter histogram)
          (expect (observability-kit:metric-snapshot-type counter)
                  :to-be
                  :counter)
          (expect (observability-kit:metric-sample-value counter-sample)
                  :to-be
                  1)
          (expect (observability-kit:metric-sample-count histogram-sample)
                  :to-be
                  1)
          (expect (observability-kit:metric-sample-sum histogram-sample)
                  :to-be
                  0.25d0)))))

  (it "inherits the handler through composed resilience calls"
    (let ((observability (make-resilience-observability)))
      (expect
       (with-resilience-observability (observability)
         (call-with-resilience (lambda () :ok)
                               :operation
                               :read))
       :to-be
       :ok)
      (let* ((snapshot
               (%observability-metric
                (resilience-observability-registry observability)
                "resilience_events_total"))
             (labels '(("event_type" . "operation-complete")
                       ("operation" . "read")))
             (sample (%observability-sample snapshot labels)))
        (expect sample :to-be-truthy)
        (when sample
          (expect (observability-kit:metric-sample-value sample)
                  :to-be
                  1)))))

  (it "accepts a supplied registry and normalizes non-symbol labels"
    (let* ((registry (observability-kit:make-metric-registry))
           (observability
             (make-resilience-observability
              :registry registry
              :cardinality-limit 4)))
      (dolist (event
                (list
                 (make-resilience-event :type nil :operation "")
                 (make-resilience-event :type 42 :operation "write")))
        (record-resilience-event observability event))
      (let* ((counter (%observability-metric
                       registry
                       "resilience_events_total"))
             (unknown-sample
               (%observability-sample
                counter
                '(("event_type" . "unknown")
                  ("operation" . "unknown"))))
             (number-sample
               (%observability-sample
                counter
                '(("event_type" . "42")
                  ("operation" . "write")))))
        (expect counter :to-be-truthy)
        (when counter
          (expect (observability-kit:metric-snapshot-type counter)
                  :to-be
                  :counter)
          (expect (observability-kit:metric-sample-value unknown-sample)
                  :to-be
                  1)
          (expect (observability-kit:metric-sample-value number-sample)
                  :to-be
                  1)))))

  (it "ignores overflowing durations and validates input types"
    (let ((observability (make-resilience-observability)))
      (record-resilience-event
       observability
       (make-resilience-event
        :type :attempt
        :operation :read
        :duration (expt 10 10000)))
      (let ((histogram
              (%observability-metric
               (resilience-observability-registry observability)
               "resilience_event_duration_seconds")))
        (expect (observability-kit:metric-snapshot-samples histogram)
                :to-be
                nil))
      (expect
       (expect-condition
        (lambda ()
          (make-resilience-observability :registry :invalid))
        'type-error)
       :to-be-truthy)
      (expect
       (expect-condition
        (lambda ()
          (record-resilience-event observability :invalid))
        'type-error)
       :to-be-truthy)
      (expect
       (expect-condition
        (lambda ()
          (resilience-observability-handler :invalid))
        'type-error)
       :to-be-truthy)))

  (it "prefers an explicit handler over inherited metrics"
    (let ((observability (make-resilience-observability))
          (events nil))
      (with-resilience-observability (observability)
        (call-with-resilience
         (lambda () :ok)
         :operation :read
         :event-handler
         (lambda (event)
           (push event events))))
      (expect (length events) :to-be 3)
      (let ((counter
              (%observability-metric
               (resilience-observability-registry observability)
               "resilience_events_total")))
        (expect (observability-kit:metric-snapshot-samples counter)
                :to-be
                nil))))

  (it "allows inherited handlers to be disabled dynamically"
    (let ((events nil))
      (with-resilience-event-handler
          ((lambda (event)
             (push event events)))
        (with-resilience-event-handler (nil)
          (call-with-resilience (lambda () :ok)
                                :operation :read)))
      (expect events :to-be nil)))

  (it-each
      ((:attempt :read)
       (:operation-failed :write)
       (:operation-complete :delete))
      "normalizes symbol event labels"
      (event-type operation)
    (let* ((observability (make-resilience-observability))
           (event (make-resilience-event
                   :type event-type
                   :operation operation))
           (labels
             (list (cons "event_type"
                         (string-downcase (symbol-name event-type)))
                   (cons "operation"
                         (string-downcase (symbol-name operation))))))
      (record-resilience-event observability event)
      (let* ((snapshot
               (%observability-metric
                (resilience-observability-registry observability)
                "resilience_events_total"))
             (sample (%observability-sample snapshot labels)))
        (expect sample :to-be-truthy))))

  (it "renders arbitrary event labels without assuming symbol types"
    (let* ((observability (make-resilience-observability))
           (event-type (list :batch 7))
           (operation (vector :read 2))
           (event (make-resilience-event
                   :type event-type
                   :operation operation)))
      (record-resilience-event observability event)
      (let* ((snapshot
               (%observability-metric
                (resilience-observability-registry observability)
                "resilience_events_total"))
             (labels
               (list (cons "event_type" (princ-to-string event-type))
                     (cons "operation" (princ-to-string operation))))
             (sample (%observability-sample snapshot labels)))
        (expect sample :to-be-truthy))))

  (it-fuzz "normalizes generated label combinations through cl-weave generators"
      ((event-type (gen-member (list nil "" :attempt :operation-complete 42)))
       (operation (gen-member (list nil "" :read :write "checkout" 7))))
      (:trials 16 :timeout-per-trial 1)
    (let* ((observability (make-resilience-observability))
           (event (make-resilience-event
                   :type event-type
                   :operation operation))
           (snapshot-name "resilience_events_total")
           (labels
             (list (cons "event_type"
                         (%expected-observability-label event-type))
                   (cons "operation"
                         (%expected-observability-label operation)))))
      (record-resilience-event observability event)
      (let* ((snapshot
               (%observability-metric
                (resilience-observability-registry observability)
                snapshot-name))
             (sample (%observability-sample snapshot labels)))
        (expect sample :to-be-truthy)
        (when sample
          (expect (observability-kit:metric-sample-value sample)
                  :to-be
                  1)))))

  (it "does not observe absent or negative durations"
    (let* ((observability (make-resilience-observability))
           (handler (resilience-observability-handler observability)))
      (funcall handler
               (make-resilience-event
                :type :attempt
                :operation :read
                :duration nil))
      (funcall handler
               (make-resilience-event
                :type :attempt
                :operation :read
                :duration -1d0))
      (let ((histogram
              (%observability-metric
               (resilience-observability-registry observability)
               "resilience_event_duration_seconds")))
        (expect (observability-kit:metric-snapshot-samples histogram)
                :to-be
                nil)))))

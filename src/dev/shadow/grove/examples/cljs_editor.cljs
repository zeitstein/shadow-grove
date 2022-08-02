(ns shadow.grove.examples.cljs-editor
  (:require
    ["codemirror" :as cm]
    ["codemirror/mode/clojure/clojure"]
    ["parinfer-codemirror" :as par-cm]
    [clojure.string :as str]
    [shadow.arborist.protocols :as ap]
    [shadow.arborist.common :as common]
    [shadow.arborist.dom-scheduler :as ds]
    [shadow.grove :as sg]
    [shadow.grove.components :as comp]
    [shadow.grove.protocols :as gp]))

(deftype EditorRoot
  [env
   marker
   ^:mutable opts
   ^:mutable editor
   ^:mutable editor-el]

  ap/IManaged
  (supports? [this next]
    (ap/identical-creator? opts next))

  (dom-sync! [this next-opts]
    (let [{:keys [value cm-opts]} next-opts]

      (when (and editor (seq value) (not= value (.getValue editor)))
        (.setValue editor value))

      (reduce-kv
        (fn [_ key val]
          (.setOption editor (name key) val))
        nil
        cm-opts)

      (set! opts next-opts)
      ))

  (dom-insert [this parent anchor]
    (.insertBefore parent marker anchor))

  (dom-first [this]
    (or editor-el marker))

  ;; codemirror doesn't render correctly if added to an element
  ;; that isn't actually in the dcoument, so we delay construction until actually entered
  ;; codemirror also does a bunch of force layouts/render when mounting
  ;; which kill performance quite badly
  (dom-entered! [this]
    (ds/write!
      (let [{:keys [editor-ref value cm-opts clojure]}
            opts

            ;; FIXME: this config stuff needs to be cleaned up, this is horrible
            cm-opts
            (js/Object.assign
              #js {:lineNumbers true
                   :theme "github"}
              (when cm-opts (clj->js cm-opts))
              (when-not (false? clojure)
                #js {:mode "clojure"
                     :matchBrackets true})
              (when (seq value)
                #js {:value value}))

            ed
            (cm.
              (fn [el]
                (set! editor-el el)
                (.insertBefore (.-parentElement marker) el marker))
              cm-opts)

            ;; FIXME: this sucks
            submit-fn
            (fn [e]
              (let [val (str/trim (.getValue ed))]
                (when (seq val)
                  (let [comp (comp/get-component env)]
                    (gp/handle-event! comp (assoc (:submit-event opts) :code val) e env)))))]

        (set! editor ed)

        (when editor-ref
          (vreset! editor-ref ed))

        ;; FIXME: this sucks, find a better way to handle configuration like this
        (when (:submit-event opts)
          (.setOption ed "extraKeys"
            #js {"Ctrl-Enter" submit-fn
                 "Shift-Enter" submit-fn}))

        #_(when-not (false? clojure)
            (par-cm/init ed)))))

  (destroy! [this dom-remove?]

    (when-some [editor-ref (:editor-ref opts)]
      (vreset! editor-ref nil))

    (when dom-remove?
      (when editor-el
        ;; FIXME: can't find a dispose method on codemirror?
        (.remove editor-el))
      (.remove marker))))

(defn make-editor [opts env]
  (EditorRoot. env (common/dom-marker env) opts nil nil))

(defn editor [opts]
  (with-meta opts {`ap/as-managed make-editor}))


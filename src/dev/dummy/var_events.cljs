(ns dummy.var-events
  (:require
   [shadow.grove :as sg :refer (<< defc)]))

(defonce rt-ref
  (sg/get-runtime ::rt))

(defn handler2 [env & args]
  env)

(defn handler3 [env & args]
  env)

(defn handler1 [env {:keys [id] :as args} & more]
  ;; fn composition instead of returning {:dispatch-n [...]}
  (-> env
      (update-in [:db id] inc)
      (handler3 :arg1)))

;; * get handler identity for logging/devtools/wire/etc surviving :advanced
;; can be packaged into a macro together with defn, e.g. sg/defev
;;
;; attach an id without using a registry
;; (not sure how a registry would even work here!)
(set! (.-shadow_grove_ev_id ^js handler1) ::handler1)
;; rejected
#_(defn click-handler ([] ::handler1) ([env ev]))

;; support current paradigm in example
(sg/reg-event rt-ref ::handler1 handler1)

(defc ui-btn [id on-click]
  (bind count (sg/kv-lookup :db id))
  (hook (sg/render-effect
         (fn [_] (js/console.log "rendering" id))))
  (<< [:div
       (str id " " count " ")
       [:button {:on-click on-click} "inc!"]]))

(defc ui-root []
  (bind count-all (sg/query #(->> % :db (vals) (reduce +))))

  (<<
   [:div (str "all " count-all)]
   [:div (sg/simple-seq
          ;; needlessly re-renders because of anon fn
          [[:id1 (fn [e env] (sg/run-tx env #(handler1 % {:id :id1})))]
           ;; current map ev
           [:id2 {:e ::handler1 :id :id2 :e/stop true}]
           ;; doesn't re-render needlessly
           ;; keeps the 'ev as maps' paradigm (smallest change)
           ;; handler1 needs to be defined (possibly :required)
           ;; lint + compilation checks + jump to definition available, unlike keywords
           [:id3 {:f handler1 :id :id3 :e/stop true}]
           ;; doesn't re-render needlessly (same as above)
           ;; supports user-defined fn signatures, unlike above
           ;; TODO: no :e/stop, etc.
           [:id4 [handler1 {:id :id4} :arg2]]
           ;; useful to support dispatching multiple independent events
           ;; but is this actually a good idea?
           [:id5 [[handler1 {:id :id5} :arg2]
                  [handler2 {:id :id5 :other 'arg-click-handler-doesnt-know}]
                  {:e ::foo}]]]
          (fn [[id on-click]] (ui-btn id on-click)))]))

(defonce root-el
  (js/document.getElementById "root"))

(defn ^:dev/after-load start []
  (sg/render rt-ref root-el (ui-root)))

(defn init []
  (sg/add-kv-table rt-ref :db {} {:id1 0 :id2 0 :id3 0 :id4 0 :id5 0})
  (start))

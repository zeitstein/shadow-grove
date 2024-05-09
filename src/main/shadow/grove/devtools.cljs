(ns shadow.grove.devtools
  {:dev/always true}
  (:require
    [shadow.cljs.modern :refer (js-await)]
    [shadow.grove.db :as db]
    [shadow.grove.history :as history]
    [shadow.grove.http-fx :as http-fx]
    [shadow.grove.impl :as impl]
    [shadow.grove.runtime :as rt]
    [shadow.grove.events :as ev]
    [shadow.grove.devtools :as-alias m]
    [shadow.grove.devtools.ui :as ui]
    [shadow.grove.devtools.relay-ws :as relay-ws]
    [shadow.grove :as sg :refer (defc << css)]
    [shadow.grove.transit :as transit]))

(defonce root-el
  (js/document.getElementById "root"))

(defonce rt-ref
  (sg/get-runtime ::m/ui))

(defn render []
  (sg/render rt-ref root-el (ui/ui-root)))

(defn register-events! []
  (ev/register-events! rt-ref))

(defn ^:dev/after-load reload! []
  (register-events!)
  (render))

(defn init []

  (sg/add-db-type rt-ref ::m/target
    {:primary-key :client-id})

  (sg/add-db-type rt-ref ::m/event
    {:primary-key :event-id})

  (sg/db-init rt-ref
    {::m/selected #{}})

  (register-events!)

  (transit/init! rt-ref)

  #_(history/init! env/rt-ref
      {:start-token "/dashboard"
       :use-fragment true
       :root-el root-el})

  (sg/reg-fx rt-ref :shadow-api
    (http-fx/make-handler
      {:on-error {:e ::m/request-error!}
       :base-url "/api"
       :request-format :transit}))

  (when ^boolean js/goog.DEBUG
    ;; can't use devtools for this since it creates a recursive infinite loop
    ;; inspecting its own data causes more data, which then causes more data, ...
    ;; being limited to console.log sucks
    (set! impl/tx-reporter
      (fn [report]
        (let [e (-> report :event :e)]
          (case e
            ::relay-ws
            (js/console.log "[WS]" (-> report :event :msg :op) (-> report :event :msg) report)
            (js/console.log e report))))))

  (when-some [search js/document.location.search]
    (let [params (js/URLSearchParams. search)]
      (when-some [rt-id (.get params "runtime")]
        (let [ident (db/make-ident ::m/target (js/parseInt rt-id 10))]
          (sg/run-tx! rt-ref
            {:e ::m/select-target!
             :target ident})

          (when-some [node-id (.get params "component")]
            (sg/run-tx! rt-ref
              {:e ::m/set-selection!
               :target ident
               :v #{node-id}})
            )))))

  (js-await [req (js/fetch "/api/token")]
    (js-await [server-token (.text req)]
      (relay-ws/init rt-ref server-token
        (fn []
          ))))

  (render))
(ns dummy.ident-config
  (:require
   [shadow.grove :as sg :refer (defc <<)]
   [shadow.grove.db :as db]
   [shadow.grove.runtime :as rt]
   [shadow.grove.events :as ev]
   [shadow.grove.db.ident-protocol :as idp]
   [clojure.string :as str]))

(defn id? [thing]
  (or (and (string? thing)
           (> (count thing) 2)
           (= "-" (subs thing 1 2))
           #_(= id-length (count thing)) ;; FIXME: can't use with test data
           )

      (and (keyword? thing)
           (nil? (namespace thing))
           (id? (name thing)))))

(extend-protocol idp/IdentProtocol
  string
  (-ident-key [thing]
    (keyword (subs thing 0 1)))

  (-ident-val [thing]
    (subs thing 2))

  cljs.core/Keyword
  (-make-ident [type id]
    (js/console.log "type" type)
    (str (subs (name type) 0 1) "-" id))

  default
  (-ident? [thing]
    (js/console.log "ident?" thing)
    (id? thing)))




(defonce data-ref
  (-> {}
      (db/configure {:block {:type :entity :primary-key :xt/id}})
      (atom)))

(defonce rt-ref
  (sg/prepare {} data-ref ::runtime-id))


(ev/reg-event rt-ref :test-tx-fn
              (fn [env {:keys [f]}]
                (f env)))



(def gid "b-123")

(comment
  (db/ident? "b-123")
  (db/make-ident :b 123)
  @@data-ref)



(defc ui-root []

  (bind {:keys [data]} (sg/query-ident gid))

  (<<
   [:div {:on-click ::add} "add"]
   [:div {:on-click ::rem} "rem"]

   [:div (str data)])

  (event ::add [env]
         (sg/run-tx env
                    {:e :test-tx-fn
                     :f (fn [env]
                          (let [env
                                (update env :db db/add :block {:xt/id "123"
                                                               :data "data123"})]
                            (js/console.log "add" (:db env))
                            env))}))

  (event ::rem [env]
         (sg/run-tx env
                    {:e :test-tx-fn
                     :f (fn [env]
                          (let [env
                                (update env :db db/remove gid)]
                            (js/console.log "rem" (:db env))
                            env))})))

(defonce root-el
  (js/document.getElementById "root"))

(defn ^:dev/after-load start []
  (sg/render rt-ref root-el (ui-root)))

(defn init []
  (start))

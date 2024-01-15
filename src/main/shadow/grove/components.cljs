(ns shadow.grove.components
  (:require-macros [shadow.grove.components])
  (:require
    [goog.object :as gobj]
    [shadow.cljs.modern :refer (defclass)]
    [shadow.arborist :as sa]
    [shadow.arborist.common :as common]
    [shadow.arborist.protocols :as ap]
    [shadow.arborist.attributes :as a]
    [shadow.grove.runtime :as rt]
    [shadow.grove.protocols :as gp]))

(def ^{:tag boolean
       :jsdoc ["@define {boolean}"]}
  DEBUG
  (js/goog.define "shadow.grove.components.DEBUG" js/goog.DEBUG))

;; this file is an exercise in writing the least idiomatic clojure code possible
;; shield your eyes and beware!

(defonce components-ref (atom {}))
(defonce instances-ref (atom #{}))

(set! *warn-on-infer* false)

(defn debug-find-roots []
  (reduce
    (fn [all instance]
      (if (::parent (.-component-env instance))
        all
        (conj all instance)))
    []
    @instances-ref))

(defn debug-component-seq
  ([]
   (debug-component-seq (first (debug-find-roots))))
  ([root]
   (tree-seq
     (fn [component]
       true)
     (fn [component]
       (.-child-components component))
     root)))

(defn debug-find-suspended []
  (->> (debug-component-seq)
       (filter #(.-suspended? %))
       (vec)))

(set! *warn-on-infer* true)

(defn get-component [env]
  (::component env))

;; called on start for hot-reload purposes
;; otherwise components may decide to skip rendering and preventing nested UI updates
;; will be stripped in release builds
(defn mark-all-dirty! []
  (doseq [^ManagedComponent comp @instances-ref]
    (.set-render-required! comp)))

(declare ^{:arglists '([x])} component-init?)
(declare ComponentInit)

(defn- make-component-init [component args]
  ;; FIXME: maybe use array, never directly accessible anyways
  {:pre [(vector? args)]}
  (ComponentInit. component args))

(extend-type gp/ComponentConfig
  cljs.core/IFn
  (-invoke
    ([this]
     (make-component-init this []))
    ([this a1]
     (make-component-init this [a1]))
    ([this a1 a2]
     (make-component-init this [a1 a2]))
    ([this a1 a2 a3]
     (make-component-init this [a1 a2 a3]))
    ([this a1 a2 a3 a4]
     (make-component-init this [a1 a2 a3 a4]))
    ([this a1 a2 a3 a4 a5]
     (make-component-init this [a1 a2 a3 a4 a5]))
    ([this a1 a2 a3 a4 a5 a6]
     (make-component-init this [a1 a2 a3 a4 a5 a6]))
    ;; FIXME: add more, user should really use maps at this point
    ))

(defn component-config? [x]
  (instance? gp/ComponentConfig x))

(defn safe-inc [x]
  (if (nil? x)
    0
    (inc x)))

(set! *warn-on-infer* false)

(defn sort-fn [^ManagedComponent a ^ManagedComponent b]
  (compare
    (-> a (.-component-env) (::depth))
    (-> b (.-component-env) (::depth))
    ))

;; FIXME: there are likely faster ways to do this
(defn find-first-set-bit-idx [search]
  {:pre [(not (zero? search))]}
  (loop [search search
         idx 0]
    (if (identical? 1 (bit-and search 1))
      idx
      (recur (bit-shift-right search 1) (inc idx)))))

(deftype EffectHook
  [^:mutable deps
   ^:mutable ^function callback
   ^:mutable callback-result
   ^:mutable ^boolean should-call?
   ^:mutable ^not-native component-handle]

  gp/IHook
  (hook-init! [this ch]
    (set! component-handle ch))

  (hook-ready? [this] true)
  (hook-value [this] ::effect-hook)
  (hook-update! [this] false)

  (hook-deps-update! [this ^EffectHook new]
    ;; comp-did-update! will call it
    ;; FIXME: (sg/effect :mount (fn [] ...)) is only called once ever
    ;; should it be called in case it uses other hook data?
    (set! callback (.-callback new))

    ;; run after each render
    ;; (sg/effect :render (fn [env] ...))

    ;; run once on mount, any constant really works
    ;; (sg/effect :mount (fn [env] ...))

    ;; when when [a b] changes
    ;; (sg/effect [a b] (fn [env] ....))

    (let [new-deps (.-deps new)]
      (when (not= new-deps :render)
        (set! should-call? (not= deps new-deps))
        (set! deps new-deps)))

    ;; doesn't have a usable output
    false)

  (hook-destroy! [this]
    (when (fn? callback-result)
      (callback-result)))

  gp/IHookDomEffect
  (hook-did-update! [this ^boolean did-render?]
    (when (and did-render? should-call?)
      (when (fn? callback-result)
        (callback-result))

      (set! callback-result (callback (gp/get-component-env component-handle)))

      (when (not= deps :render)
        (set! should-call? false))
      )))

(deftype ComponentHookHandle [^not-native component idx]
  gp/IEnvSource
  (get-component-env [this]
    (.-component-env component))

  gp/ISchedulerSource
  (get-scheduler [this]
    (.-scheduler component))

  gp/IComponentHookHandle
  (hook-invalidate! [this]
    (.invalidate-hook! component idx)))

(deftype SimpleVal [^:mutable val]
  gp/IHook
  (hook-init! [this ch])
  (hook-ready? [this] true)
  (hook-value [this] val)
  (hook-update! [this])
  (hook-deps-update! [this ^SimpleVal next]
    (let [new-val (.-val next)
          updated? (not= new-val val)]
      (set! val new-val)
      updated?))
  (hook-destroy! [this]))

(defn maybe-wrap-val [val]
  (if (implements? gp/IHook val)
    val
    (SimpleVal. val)))

(defclass ManagedComponent
  (field ^not-native scheduler)
  (field ^not-native parent-env)
  (field ^not-native component-env)
  (field ^ComponentConfig config)
  (field root)
  (field args)
  (field rendered-args)
  (field ^number current-idx (int 0))
  (field ^array hooks)
  (field ^array hooks-with-effects #js [])
  (field ^number dirty-from-args (int 0))
  (field ^number dirty-hooks (int 0))
  (field ^number updated-hooks (int 0))
  (field ^boolean needs-render? true) ;; initially needs a render
  (field ^boolean suspended? false)
  (field ^boolean destroyed? false)
  (field ^boolean error? false)
  (field ^boolean dom-entered? false)
  (field work-set (js/Set.)) ;; sub-tree pending work

  (constructor [this e c a]
    (set! parent-env e)
    (set! config c)
    (set! args a)
    (set! scheduler (::scheduler parent-env))
    (when DEBUG
      ;; only keeping this info for debugging purposes currently, don't think its needed otherwise
      ;; use js/Set since it always maintains insertion order which makes debugging easier
      (set! this -child-components (js/Set.))
      (when-some [parent (::component parent-env)]
        (.. parent -child-components (add this))))

    (set! component-env
      (-> parent-env
          (update ::depth safe-inc)
          (assoc ::parent (::component parent-env)
                 ::ap/dom-event-handler this
                 ::component this
                 ::event-target this
                 ::scheduler this)))

    ;; marks component boundaries in dev mode for easier inspect
    (when DEBUG
      (swap! instances-ref conj this)
      (set! (.-marker-before this)
        (doto (js/document.createComment (str "component: " (.-component-name config)))
          (set! -shadow$instance this)))
      (set! (.-marker-after this)
        (doto (js/document.createComment (str "/component: " (.-component-name config)))
          (set! -shadow$instance this))))

    (set! root (common/managed-root component-env))
    (set! hooks (js/Array. (alength (.-hooks config)))))

  cljs.core/IHash
  (-hash [this]
    (goog/getUid this))

  ap/IManaged
  (dom-first [this]
    (if DEBUG
      (.-marker-before this)
      (ap/dom-first root)))

  (dom-insert [this parent anchor]
    (when DEBUG
      (.insertBefore parent (.-marker-before this) anchor))
    (ap/dom-insert root parent anchor)
    (when DEBUG
      (.insertBefore parent (.-marker-after this) anchor)))

  (dom-entered! [this]
    (set! dom-entered? true)
    (when-not error?
      (ap/dom-entered! root)
      ;; trigger first on mount
      (.did-update! this true)))

  (supports? [this next]
    (and (component-init? next)
         (let [other (.-component ^ComponentInit next)]
           (identical? config other))
         ;; (defc ui-thing [^:stable ident] ...)
         ;; should cause unmount on changing ident
         (let [stable-args (-> config .-opts (get ::stable-args))]
           (or (nil? stable-args)
               (let [old-args args
                     new-args (.-args next)]
                 (every? #(= (nth old-args %) (nth new-args %)) stable-args))))
         (let [custom-check (-> config .-opts (get :supports?))]
           (or (nil? custom-check)
               (custom-check args (.-args next))))))

  (dom-sync! [this ^ComponentInit next]
    (. config (check-args-fn this args (.-args next)))
    (set! args (.-args next))
    (when (.work-pending? this)
      (.schedule! this ::dom-sync!)))

  (destroy! [this ^boolean dom-remove?]
    (.unschedule! this)
    (when DEBUG
      (swap! instances-ref disj this)

      (when-some [parent (::parent component-env)]
        (.. parent -child-components (delete this)))

      (when dom-remove?
        (.remove (.-marker-before this))
        (.remove (.-marker-after this))))

    (set! destroyed? true)

    (.forEach hooks
      (fn [^not-native hook]
        (when hook
          (gp/hook-destroy! hook))))

    (ap/destroy! root dom-remove?))

  ;; FIXME: figure out default event handler
  ;; don't want to declare all events all the time
  gp/IHandleEvents
  (handle-event! [this {ev-id :e :as ev-map} e origin]
    (let [handler
          (cond
            (keyword? ev-id)
            (or (get (.-events config) ev-id)
                (get (.-opts config) ev-id))

            :else
            (throw (ex-info "unknown event" {:event ev-map})))]

      (if handler
        (handler component-env ev-map e origin)

        ;; no handler, try parent
        (if-some [parent (::event-target parent-env)]
          (gp/handle-event! parent ev-map e origin)
          (js/console.warn "event not handled" ev-id ev-map)))))

  gp/IScheduleWork
  (did-suspend! [this work-task]
    (gp/did-suspend! scheduler work-task))

  (did-finish! [this work-task]
    (gp/did-finish! scheduler work-task))

  (schedule-work! [this work-task trigger]
    (when (zero? (.-size work-set))
      (gp/schedule-work! scheduler this trigger))

    (.add work-set work-task))

  (unschedule! [this work-task]
    (.delete work-set work-task)

    (when (zero? (.-size work-set))
      (gp/unschedule! scheduler this)))

  (run-now! [this callback trigger]
    (gp/run-now! scheduler callback trigger))

  ;; parent tells us to work
  gp/IWork
  (work! [this]
    (when-not error?
      (try
        ;; always complete our own work first
        ;; a re-render may cause the child tree to change
        ;; and maybe some work to disappear
        (while ^boolean (.work-pending? this)
          (.run-next! this))

        ;; FIXME: only process children when this is done and not suspended?
        (let [iter (.values work-set)]
          (loop []
            (let [current (.next iter)]
              (when (not ^boolean (.-done current))
                (gp/work! ^not-native (.-value current))

                ;; should time slice later and only continue work
                ;; until a given time budget is consumed
                (recur)))))
        (catch :default ex
          (.handle-error! this ex)))))

  ;; FIXME: should have an easier way to tell shadow-cljs not to create externs for these
  Object
  (handle-error! [this ex]
    (set! error? true)
    (.unschedule! this)

    (let [err-fn (::error-handler parent-env)]
      (err-fn this ex)))

  (get-hook-value [this idx]
    (gp/hook-value (aget hooks idx)))

  (invalidate-hook! [this idx]
    ;; (js/console.log "invalidate-hook!" idx current-idx (.-component-name config) this)

    (set! dirty-hooks (bit-set dirty-hooks idx))

    ;; don't set higher when currently at lower index, would otherwise skip work
    (set! current-idx (js/Math.min idx current-idx))

    ;; always need to resume so the invalidated hooks can do work
    ;; could check if actually suspended but no need
    (set! suspended? false)

    (.schedule! this ::hook-invalidate!))

  (mark-hooks-dirty! [this dirty-bits]
    (set! dirty-hooks (bit-or dirty-hooks dirty-bits))
    (set! current-idx (find-first-set-bit-idx dirty-hooks)))

  (mark-dirty-from-args! [this dirty-bits]
    (set! dirty-from-args (bit-or dirty-from-args dirty-bits))
    (.mark-hooks-dirty! this dirty-bits))

  (set-render-required! [this]
    (set! needs-render? true)
    (set! current-idx (js/Math.min current-idx (alength (.-hooks config)))))

  (run-next! [^not-native this]
    #_(js/console.log "Component:run-next!"
        (.-component-name config)
        current-idx
        (alength (.-hooks config))
        dirty-hooks
        (bit-test dirty-hooks current-idx)
        this)
    (if (identical? current-idx (alength (.-hooks config)))
      ;; all hooks done
      (.component-render! this)

      ;; process hooks in order, starting at the lowest index invalidated
      (let [hook (aget hooks current-idx)]
        ;; (js/console.log "Component:run-next!" current-idx (:component-name config) (pr-str (type hook)) this)

        (cond
          ;; doesn't exist, create it
          (not hook)
          (let [^function run-fn (-> (.-hooks config) (aget current-idx) (.-run))
                handle (ComponentHookHandle. this current-idx)
                val (run-fn this)
                hook (maybe-wrap-val val)]

            ;; (js/console.log "Component:init-hook!" (:component-name config) current-idx val hook)

            (aset hooks current-idx hook)

            (gp/hook-init! hook handle)

            ;; previous hook may have marked hook as dirty since it used data
            ;; but hook may have not been constructed yet, constructing must clear dirty bit
            (set! dirty-hooks (bit-clear dirty-hooks current-idx))
            ;; construction counts as updated since value became available for first time
            (set! updated-hooks (bit-set updated-hooks current-idx))

            (when (bit-test (.-render-deps config) current-idx)
              (set! needs-render? true))

            (when (satisfies? gp/IHookDomEffect hook)
              (.push hooks-with-effects hook))

            (if (gp/hook-ready? hook)
              (set! current-idx (inc current-idx))
              (.suspend! this current-idx)))

          ;; marked dirty, update it
          ;; make others dirty if actually updated
          (bit-test dirty-hooks current-idx)
          (let [hook-config (aget (.-hooks config) current-idx)

                deps-updated?
                ;; dirty hooks this depends-on should trigger an update
                ;; or changed args used by this should trigger
                (or (pos? (bit-and (.-depends-on hook-config) updated-hooks))
                    (bit-test dirty-from-args current-idx))

                ^function run (.-run hook-config)

                next-hook
                (maybe-wrap-val (run this))

                _ (when-not (identical? (type hook) (type next-hook))
                    (throw (ex-info "illegal hook value, type cannot change" {:old hook :new next-hook})))

                did-update? ;; checks if hook deps changed as well, calling init again
                (if deps-updated?
                  (gp/hook-deps-update! hook next-hook)
                  (gp/hook-update! hook))]

            #_(js/console.log "Component:hook-update!"
                (:component-name config)
                current-idx
                deps-updated?
                did-update?
                hook)

            (set! dirty-hooks (bit-clear dirty-hooks current-idx))

            (when did-update?
              (set! updated-hooks (bit-set updated-hooks current-idx))
              (set! dirty-hooks (bit-or dirty-hooks (.-affects hook-config)))

              (when (bit-test (.-render-deps config) current-idx)
                (set! needs-render? true)))

            (if (gp/hook-ready? hook)
              (set! current-idx (inc current-idx))
              (.suspend! this current-idx)))

          :else
          (set! current-idx (inc current-idx))))))

  (work-pending? [this]
    (and (not destroyed?)
         (not suspended?)
         (not error?)
         (or (pos? dirty-hooks)
             needs-render?
             (>= (alength (.-hooks config)) current-idx))))

  (suspend! [this hook-causing-suspend]
    ;; (js/console.log "suspending" hook-causing-suspend this)

    ;; just in case we were already scheduled. should really track this more efficiently
    (.unschedule! this)
    (gp/did-suspend! scheduler this)
    (set! suspended? true))

  (schedule! [this trigger]
    (when-not destroyed?
      (gp/schedule-work! scheduler this trigger)))

  (unschedule! [this]
    (gp/unschedule! scheduler this))

  (component-render! [^ManagedComponent this]
    (assert (zero? dirty-hooks) "Got to render while hooks are dirty")
    ;; (js/console.log "Component:render!" (.-component-name config) updated-hooks needs-render? suspended? destroyed? this)
    (set! updated-hooks (int 0))
    (set! dirty-from-args (int 0))

    (let [did-render? needs-render?]
      (when needs-render?
        (let [frag (. config (render-fn this))]

          (set! rendered-args args)
          (set! needs-render? false)

          (ap/update! root frag)))

      ;; only trigger dom effects when mounted
      (when dom-entered?
        (.did-update! this did-render?)))

    ;; must keep this for work scheduling so it knows its done
    (set! current-idx (inc current-idx))

    (gp/did-finish! scheduler this)
    (.unschedule! this))

  (did-update! [this did-render?]
    (.forEach hooks-with-effects
      (fn [^not-native item]
        (gp/hook-did-update! item did-render?)))))

;; FIXME: no clue why I can't put this in ManagedComponent directly
;; compiler complains with undeclared var ManagedComponent
;; probably something in defclass I missed

(extend-type ManagedComponent
  ap/IHandleDOMEvents
  (validate-dom-event-value! [this env event ev-value]
    (when-not (or (keyword? ev-value) (map? ev-value))
      (throw
        (ex-info
          (str "event: " event " expects a map or keyword value")
          {:event event :value ev-value}))))

  ;; event is "click" for :on-click etc which we just drop
  (handle-dom-event! [this event-env event ev-value dom-event]
    (let [ev-map
          (if (map? ev-value)
            ev-value
            {:e ev-value})]

      ;; (js/console.log "dom-event" this event-env event ev-map dom-event)
      (gp/run-now! (.-scheduler this)
        #(gp/handle-event! this ev-map dom-event event-env)
        ::handle-dom-event!))))

(set! *warn-on-infer* true)

(defn component-create [env ^gp/ComponentConfig config args]
  (when ^boolean js/goog.DEBUG
    (when-not (instance? gp/ComponentConfig config)
      (throw (ex-info "not a component definition" {:config config :props args}))))

  (doto (ManagedComponent. env config args)
    ;; do as much work as possible now
    ;; only go async when suspended
    (gp/work!)))

(deftype ComponentInit [component args]
  ap/IConstruct
  (as-managed [this env]
    (component-create env component args))

  IEquiv
  (-equiv [this ^ComponentInit other]
    (and (instance? ComponentInit other)
         (identical? component (.-component other))
         (= args (.-args other)))))

(defn component-init? [x]
  (instance? ComponentInit x))

(deftype HookConfig [depends-on affects run])

(defn make-hook-config
  "used by defc macro, do not use directly"
  [depends-on affects run]
  {:pre [(nat-int? depends-on)
         (nat-int? affects)
         (fn? run)]}
  (HookConfig. depends-on affects run))

(defn make-component-config
  "used by defc macro, do not use directly"
  [component-name
   hooks
   opts
   check-args-fn
   render-deps
   render-fn
   events]
  {:pre [(string? component-name)
         (array? hooks)
         (every? #(instance? HookConfig %) hooks)
         (map? opts)
         (fn? check-args-fn)
         (nat-int? render-deps)
         (fn? render-fn)
         (map? events)]}

  (let [cfg
        (gp/ComponentConfig.
          component-name
          hooks
          opts
          check-args-fn
          render-deps
          render-fn
          events)]

    (when ^boolean js/goog.DEBUG
      (swap! components-ref assoc component-name cfg))

    cfg))

;; these are called by defc macro, do not delete!
;; cursive marks these as unused

(defn get-arg ^not-native [^ManagedComponent comp idx]
  (-nth ^not-native (.-args comp) idx))

(defn check-args! [^ManagedComponent comp new-args expected]
  (assert (>= (count new-args) expected) (str "component " (. ^ComponentConfig (. comp -config) -component-name) " expected at least " expected " arguments")))

(defn arg-triggers-hooks! [^ManagedComponent comp idx dirty-bits]
  (.mark-dirty-from-args! comp dirty-bits))

(defn arg-triggers-render! [^ManagedComponent comp idx]
  (.set-render-required! comp))

(defn get-hook-value [^ManagedComponent comp idx]
  (.get-hook-value comp idx))

(defn get-events [^ManagedComponent comp]
  ;; FIXME: ... loses typehints?
  (. ^clj (. comp -config) -events))

(defn get-parent [^ManagedComponent comp]
  (get-component (. comp -parent-env)))

(defn get-component-name [^ManagedComponent comp]
  (. ^clj (. comp -config) -component-name))


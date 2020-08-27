(ns shadow.experiments.grove
  "grove - a small wood or forested area (ie. trees)
   a mini re-frame/fulcro hybrid. re-frame event styles + somewhat normalized db"
  (:require-macros [shadow.experiments.grove])
  (:require
    [shadow.experiments.arborist.protocols :as ap]
    [shadow.experiments.arborist.common :as common]
    [shadow.experiments.arborist.fragments] ;; util macro references this
    [shadow.experiments.arborist :as sa]
    [goog.async.nextTick]
    [shadow.experiments.grove.protocols :as gp]
    [shadow.experiments.grove.components :as comp]
    [shadow.experiments.grove.ui.util :as util]
    [shadow.experiments.grove.ui.suspense :as suspense]
    [shadow.experiments.grove.ui.streams :as streams]
    [shadow.experiments.grove.ui.atoms :as atoms]
    [shadow.experiments.arborist.attributes :as a]))

;; these are private - should not be accessed from the outside
(defonce active-roots-ref (atom {}))
(defonce active-apps-ref (atom {}))

(deftype TreeScheduler [^:mutable work-arr ^:mutable update-pending?]
  gp/IScheduleUpdates
  (did-suspend! [this work-task])
  (did-finish! [this work-task])

  (schedule-update! [this work-task]
    ;; FIXME: now possible a task is scheduled multiple times
    ;; but the assumption is that task will only schedule themselves once
    ;; doesn't matter if its in the arr multiple times too much
    (.push work-arr work-task)

    ;; schedule was added in some async work
    (when-not update-pending?
      (set! update-pending? true)
      (util/next-tick #(.process-pending! this))))

  (unschedule! [this work-task]
    ;; FIXME: might be better to track this in the task itself and just check when processing
    ;; and just remove it then. the array might get long?
    (set! work-arr (.filter work-arr (fn [x] (not (identical? x work-task))))))

  (run-now! [this callback]
    (set! update-pending? true)
    (callback)
    (.process-pending! this))

  Object
  (process-pending! [this]
    ;; FIXME: this now processes in FCFS order
    ;; should be more intelligent about prioritizing
    ;; should use requestIdleCallback or something to schedule in batch
    (let [start (util/now)
          done
          (loop []
            (if-not (pos? (alength work-arr))
              true
              (let [next (aget work-arr 0)]
                (when-not (gp/work-pending? next)
                  (throw (ex-info "work was scheduled but isn't pending?" {:next next})))
                (gp/work! next)

                ;; FIXME: using this causes a lot of intermediate paints
                ;; which means things take way longer especially when rendering collections
                ;; so there really needs to be a Suspense style node that can at least delay
                ;; inserting nodes into the actual DOM until they are actually ready
                (let [diff (- (util/now) start)]
                  ;; FIXME: more logical timeouts
                  ;; something like IdleTimeout from requestIdleCallback?
                  ;; dunno if there is a polyfill for that?
                  ;; not 16 to let the runtime do other stuff
                  (when (< diff 10)
                    (recur))))))]

      (if done
        (set! update-pending? false)
        (js/goog.async.nextTick #(.process-pending! this))))

    ;; FIXME: dom effects
    ))

(defn run-now! [env callback]
  (gp/run-now! (::gp/scheduler env) callback))

(defn dispatch-up! [{::comp/keys [^not-native parent] :as env} ev-vec]
  {:pre [(map? env)
         (vector? ev-vec)
         (qualified-keyword? (first ev-vec))]}
  ;; FIXME: should schedule properly when it isn't in event handler already
  (gp/handle-event! parent ev-vec nil))

(deftype QueryHook
  [^:mutable ident
   ^:mutable query
   ^:mutable config
   component
   idx
   env
   query-engine
   query-id
   ^:mutable ready?
   ^:mutable read-result]

  gp/IBuildHook
  (hook-build [this c i]
    ;; support multiple query engines by allowing queries to supply which key to use
    (let [env (comp/get-env c)
          engine-key (:engine config ::gp/query-engine)
          query-engine (get env engine-key)]
      (assert query-engine (str "no query engine in env for key " engine-key))
      (QueryHook. ident query config c i env query-engine (util/next-id) false nil)))

  gp/IHook
  (hook-init! [this]
    (.set-loading! this)
    (.register-query! this))

  (hook-ready? [this]
    (or (false? (:suspend config)) ready?))

  (hook-value [this]
    read-result)

  ;; node deps changed, check if query changed
  (hook-deps-update! [this ^QueryHook val]
    (if (and (= ident (.-ident val))
             (= query (.-query val))
             (= config (.-config val)))
      false
      ;; query changed, remove it entirely and wait for new one
      (do (.unregister-query! this)
          (set! ident (.-ident val))
          (set! query (.-query val))
          (set! config (.-config val))
          (.set-loading! this)
          (.register-query! this)
          true)))

  ;; node was invalidated and needs update, but its dependencies didn't change
  (hook-update! [this]
    true)

  (hook-destroy! [this]
    (.unregister-query! this))

  Object
  (register-query! [this]
    (gp/query-init query-engine query-id (if ident [{ident query}] query) config
      (fn [result]
        (.set-data! this result))))

  (unregister-query! [this]
    (gp/query-destroy query-engine query-id))

  (set-loading! [this]
    (set! ready? (false? (:suspend config)))
    (set! read-result (assoc (:default config {}) ::loading-state :loading)))

  (set-data! [this data]
    (let [data (if ident (get data ident) data)]
      (set! read-result (assoc data ::loading-state :ready))
      ;; on query update just invalidate. might be useful to bypass certain suspend logic?
      (if ready?
        (comp/hook-invalidate! component idx)
        (do (comp/hook-ready! component idx)
            (set! ready? true))))))

(defn query-ident
  ([ident query]
   (query-ident ident query {}))
  ([ident query config]
   {:pre [;; (db/ident? ident) FIXME: can't access db namespace in main, move to protocols?
          (vector? ident)
          (keyword? (first ident))
          (vector? query)
          (map? config)]}
   (QueryHook. ident query config nil nil nil nil nil false nil)))

(defn query-root
  ([query]
   (query-root query {}))
  ([query config]
   {:pre [(vector? query)
          (map? config)]}
   (QueryHook. nil query config nil nil nil nil nil false nil)))

(defn tx*
  [{::gp/keys [query-engine] :as env} tx]
  (assert query-engine "missing query-engine in env")
  (gp/transact! query-engine tx))

(defn tx [env ev-vec e]
  (tx* env ev-vec))

(defn run-tx [env tx]
  (tx* env tx))

(defn init* [app-id init-env init-features]
  {:pre [(some? app-id)
         (map? init-env)
         (sequential? init-features)
         (every? fn? init-features)]}

  (let [scheduler (TreeScheduler. (array) false)

        env
        (assoc init-env
          ::app-id app-id
          ::gp/scheduler scheduler
          ::suspense-keys (atom {}))

        env
        (reduce
          (fn [env init-fn]
            (init-fn env))
          env
          init-features)]

    env))

(defn init [app-id init-env init-features]
  {:pre [(some? app-id)
         (map? init-env)
         (sequential? init-features)
         (every? fn? init-features)]}

  (let [env (init* app-id init-env init-features)]
    ;; FIXME: throw if already initialized?
    (swap! active-apps-ref assoc app-id env)
    ;; never expose the env so people don't get ideas about using it
    app-id))

(defn start [app-id root-el root-node]
  (let [env (get @active-apps-ref app-id)]
    (if-not env
      (throw (ex-info "app not initialized" {:app-id app-id}))
      (let [active (get @active-roots-ref root-el)]
        (if-not active
          (let [root (sa/dom-root root-el env)]
            (sa/update! root root-node)
            (swap! active-roots-ref assoc root-el {:env env :root root :root-el root-el})
            ::started)

          (let [{:keys [root]} active]
            (assert (identical? env (:env active)) "can't change env between restarts")
            (when ^boolean js/goog.DEBUG
              (comp/mark-all-dirty!))

            (sa/update! root root-node)
            ::updated
            ))))))

;; FIXME: figure out if stop is ever needed?
#_(defn stop [root-el]
    (when-let [{::keys [app-root] :as env} (get @active-roots-ref root-el)]
      (swap! active-roots-ref dissoc root-el)
      (ap/destroy! app-root)
      (dissoc env ::app-root ::root-el)))

(defn watch
  "hook that watches an atom and triggers an update on change
   accepts an optional path-or-fn arg that can be used for quick diffs

   (watch the-atom [:foo])
   (watch the-atom (fn [old new] ...))"
  ([the-atom]
   (watch the-atom (fn [old new] new)))
  ([the-atom path-or-fn]
   (if (vector? path-or-fn)
     (atoms/AtomWatch. the-atom (fn [old new] (get-in new path-or-fn)) nil nil nil)
     (atoms/AtomWatch. the-atom path-or-fn nil nil nil))))

(defn env-watch
  ([key-to-atom]
   (env-watch key-to-atom [] nil))
  ([key-to-atom path]
   (env-watch key-to-atom path nil))
  ([key-to-atom path default]
   {:pre [(keyword? key-to-atom)
          (vector? path)]}
   (atoms/EnvWatch. key-to-atom path default nil nil nil nil)))

(defn suspense [opts vnode]
  (suspense/SuspenseInit. opts vnode))

(defn stream [stream-key opts item-fn]
  (streams/StreamInit. stream-key opts item-fn))

(defn simple-seq [coll render-fn]
  (sa/render-seq coll nil render-fn))

(defn render-seq [coll key-fn render-fn]
  (sa/render-seq coll key-fn render-fn))


(deftype TrackChange [^:mutable val ^:mutable trigger-fn ^:mutable result env component idx]
  gp/IBuildHook
  (hook-build [this c i]
    (TrackChange. val trigger-fn nil (comp/get-env c) c i))

  gp/IHook
  (hook-init! [this]
    (set! result (trigger-fn env nil val)))

  (hook-ready? [this] true)
  (hook-value [this] result)
  (hook-update! [this] false)

  (hook-deps-update! [this ^TrackChange new-track]
    (assert (instance? TrackChange new-track))

    (let [next-val (.-val new-track)
          prev-result result]

      (set! trigger-fn (.-trigger-fn new-track))
      (set! result (trigger-fn env val next-val))
      (set! val next-val)

      (not= result prev-result)))

  (hook-destroy! [this]
    ))

(defn track-change [val trigger-fn]
  (TrackChange. val trigger-fn nil nil nil nil))

(deftype DomRef [^:mutable current]
  cljs.core/IDeref
  (-deref [this]
    current)
  cljs.core/IFn
  (-invoke [this env val]
    (set! current val)))

(a/add-attr :dom/ref
  (fn [env node oval nval]
    (nval env node)))

(defn dom-ref []
  (DomRef. nil))

(defn effect
  "runs passed effect callback when provided deps argument changes
   effect runs after render.

   callback can return a function which will be called if cleanup is required"
  [deps callback]
  (comp/EffectHook. deps callback nil true nil nil))

(defn render-effect
  "call callback after every render"
  [callback]
  (comp/EffectHook. :render callback nil true nil nil))

(defn mount-effect
  "call callback on mount once"
  [callback]
  (comp/EffectHook. :mount callback nil true nil nil))

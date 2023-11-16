(ns shadow.grove.db-brimm
  "Fork of shadow.grove.db, removing idents and leaning more into a schema-less
   approach. ::all collections can still be used, though."
  (:refer-clojure :exclude (ident? remove)))


(defprotocol ITransaction
  (tx-log-new [this key])
  (tx-log-modified [this key])
  (tx-log-removed [this key])
  (tx-check-completed! [this]))

(defprotocol ITransactable
  (tx-begin [this])
  (tx-get [this])
  (db-schema [this]))

(defprotocol ITransactableCommit
  (tx-commit! [this]))

(defprotocol IObserved
  (observed-keys [this]))


#?(:cljs
   (set! *warn-on-infer* false))

#?(:clj
   (defn keyword-identical? [a b]
     (identical? a b)))

(defn- set-conj [x y]
  (if (nil? x)
    #{y}
    (conj x y)))

(defn coll-key [entity-type]
  [::all entity-type])

;; legacy

(defn make-ident [type id]
  (throw (ex-info "s.g.db/make-ident is deprecated in brimm-mods" {})))

(defn ident? [thing]
  (throw (ex-info "s.g.db/ident? is deprecated in brimm-mods" {})))

(defn ident-key [thing]
  (throw (ex-info "s.g.db/ident-key is deprecated in brimm-mods" {})))

(defn ident-val [thing]
  (throw (ex-info "s.g.db/ident-val is deprecated in brimm-mods" {})))



#?(:clj
   (deftype ObservedData
     [^:unsynchronized-mutable keys-used
      ^clojure.lang.IPersistentMap data]
     IObserved
     (observed-keys [_]
       (persistent! keys-used))

     clojure.lang.IMeta
     (meta [_]
       (.meta data))

     ;; FIXME: implement rest of seq functions

     clojure.lang.IPersistentMap
     (assoc [this key val]
       (throw (ex-info "read-only" {})))

     (assocEx [this key val]
       (throw (ex-info "read-only" {})))

     (without [this key]
       (throw (ex-info "read-only" {})))

     (containsKey [this key]
       (when (nil? key)
         (throw (ex-info "cannot read nil key" {})))
       (set! keys-used (conj! keys-used key))
       (.containsKey data key))

     (valAt [this key]
       (when (nil? key)
         (throw (ex-info "cannot read nil key" {})))
       (set! keys-used (conj! keys-used key))
       (.valAt data key))

     (valAt [this key not-found]
       (when (nil? key)
         (throw (ex-info "cannot read nil key" {})))
       (set! keys-used (conj! keys-used key))
       (.valAt data key not-found))

     (entryAt [this key]
       (when (nil? key)
         (throw (ex-info "cannot read nil key" {})))
       (set! keys-used (conj! keys-used key))
       (.entryAt data key)))

   :cljs
   (deftype ObservedData [^:mutable keys-used ^not-native data]
     IObserved
     (observed-keys [_]
       (persistent! keys-used))

     IMeta
     (-meta [_]
       (-meta data))

     ;; map? predicate checks for this protocol
     IMap
     (-dissoc [coll k]
       (throw (ex-info "observed data is read-only" {})))

     IAssociative
     (-contains-key? [coll k]
       (-contains-key? data k))
     (-assoc [coll k v]
       (throw (ex-info "observed data is read-only, assoc not allowed" {:k k :v v})))

     ILookup
     (-lookup [_ key]
       (when (nil? key)
         (throw (ex-info "cannot read nil key" {})))
       (set! keys-used (conj! keys-used key))
       (-lookup data key))

     (-lookup [_ key default]
       (when (nil? key)
         (throw (ex-info "cannot read nil key" {})))
       (set! keys-used (conj! keys-used key))
       (-lookup data key default))))

(deftype Transaction
  [data-before
   keys-new
   keys-updated
   keys-removed
   completed-ref]

  ITransaction
  (tx-check-completed! [this]
    (when @completed-ref
      (throw (ex-info "tx already commited!" {}))))

  (tx-log-new [this key]
    (Transaction.
      data-before
      (conj keys-new key)
      keys-updated
      keys-removed
      completed-ref))

  (tx-log-modified [this key]
    (Transaction.
      data-before
      keys-new
      (conj keys-updated key)
      keys-removed
      completed-ref))

  (tx-log-removed [this key]
    (Transaction.
      data-before
      keys-new
      keys-updated
      (conj keys-removed key)
      completed-ref))

  ITransactableCommit
  (tx-commit! [this]
    (vreset! completed-ref true)
    {:data-before data-before
     ;; not using persistent for simpler insertion of "tx rules"
     :keys-new keys-new #_(persistent! keys-new)
     :keys-updated keys-updated #_(persistent! keys-updated)
     :keys-removed keys-removed #_(persistent! keys-removed)}))

(deftype GroveDB
  #?@(:clj
      [[schema
        ^clojure.lang.IPersistentMap data
        ^Transaction tx]

       clojure.lang.IDeref
       (deref [_]
         data)

       clojure.lang.IPersistentMap
       (count [this]
         (.count data))

       (containsKey [this key]
         (when (nil? key)
           (throw (ex-info "cannot read nil key" {})))
         (.containsKey data key))

       (valAt [this key]
         (when (nil? key)
           (throw (ex-info "cannot read nil key" {})))
         (.valAt data key))

       (valAt [this key not-found]
         (when (nil? key)
           (throw (ex-info "cannot read nil key" {})))
         (.valAt data key not-found))

       (entryAt [this key]
         (when (nil? key)
           (throw (ex-info "cannot read nil key" {})))
         (.entryAt data key))

       (assoc [this key value]
         (when tx
           (tx-check-completed! tx))

         (when (nil? key)
           (throw (ex-info "nil key not allowed" {:value value})))

         ;; FIXME: should it really check each write if anything changed?
         (let [prev-val    (.valAt data key ::not-found)
               type-fn     (:type-fn schema)
               entity-type (type-fn value)
               valid-type? (some? entity-type)]
           (if (identical? prev-val value)
             this
             (if (= ::not-found prev-val)
               ;; new
               (if-not valid-type?
                 (GroveDB.
                  schema
                  (assoc data key value)
                  (when tx
                    (tx-log-new tx key)))

                 (GroveDB.
                  schema
                  (-> data
                      (assoc key value)
                      (update (coll-key entity-type) set-conj key))
                  (when tx
                    (-> tx
                        (tx-log-new key)
                        (tx-log-modified (coll-key entity-type))))))

               ;; update
               (GroveDB.
                schema
                (assoc data key value)
                (when tx
                  (tx-log-modified tx key)))

               #_(if-not valid-type?
                 (GroveDB.
                  schema
                  (assoc data key value)
                  (when tx
                    (tx-log-modified tx key)
                    #_(-> tx
                          (tx-log-new key)
                          (tx-log-modified key))))

                 (GroveDB.
                  schema
                  (.assoc data key value) ;; TODO: why is this .assoc unlike above?
                  (when tx
                    (-> tx
                        (tx-log-modified key)
                        ;; ! I think not needed
                         ;; need to update the entity-type collection since some queries might change if one in the list changes
                         ;; FIXME: this makes any update potentially expensive, maybe should leave this to the user?
                        #_(tx-log-modified (coll-key entity-type))))))))))

       (assocEx [this key value]
         (throw (ex-info "assocEx is no longer used" {})))

       (without [this key]
         (when tx
           (tx-check-completed! tx))

         (let [entity      (.valAt data key ::not-found)
               type-fn     (:type-fn schema)
               entity-type (type-fn entity)
               valid-type? (some? entity-type)]
           (GroveDB.
            schema
            (cond-> (.without data key)
              valid-type?
              (update (coll-key entity-type) disj key))
            (when tx
              (cond-> (tx-log-removed tx key)
                valid-type?
                (tx-log-modified (coll-key entity-type)))))))]

      :cljs
      [[schema
        ^not-native data
        ^not-native tx]

       IDeref
       (-deref [_]
         data)

       ILookup
       (-lookup [this key]
         (when tx
           (tx-check-completed! tx))
         (-lookup data key))

       (-lookup [this key default]
         (when tx
           (tx-check-completed! tx))
         (-lookup data key default))

       ICounted
       (-count [this]
         (when tx
           (tx-check-completed! tx))
         (-count data))

       IMap
       (-dissoc [this key]
         (when tx
           (tx-check-completed! tx))

         (let [entity      (-lookup data key ::not-found)
               type-fn     (:type-fn schema)
               entity-type (type-fn entity)
               valid-type? (some? entity-type)]
           (GroveDB.
            schema
            (cond-> (-dissoc data key)
              valid-type?
              (update (coll-key entity-type) disj key))
            (when tx
              (cond-> (tx-log-removed tx key)
                valid-type?
                (tx-log-modified (coll-key entity-type)))))))

       IAssociative
       (-contains-key? [coll k]
         (-contains-key? data k))

       (-assoc [this key value]
         (when tx
           (tx-check-completed! tx))

         (when (nil? key)
           (throw (ex-info "nil key not allowed" {:value value})))

         ;; FIXME: should it really check each write if anything changed?

         (let [prev-val    (-lookup data key ::not-found)
               type-fn     (:type-fn schema)
               entity-type (type-fn value)
               valid-type? (some? entity-type)]
           (if (identical? prev-val value)
             this
             (if (= ::not-found prev-val)
               ;; new
               (if-not valid-type?
                 (GroveDB.
                  schema
                  (-assoc data key value)
                  (when tx
                    (tx-log-new tx key)))

                 (GroveDB.
                  schema
                  (-> data
                      (-assoc key value)
                      (update (coll-key entity-type) set-conj key))
                  (when tx
                    (-> tx
                        (tx-log-new key)
                        (tx-log-modified (coll-key entity-type))))))

               ;; update
               (GroveDB.
                schema
                (-assoc data key value)
                (when tx
                  (tx-log-modified tx key)))))))

       ICollection
       (-conj [coll ^not-native entry]
         (if (vector? entry)
           (-assoc coll (-nth entry 0) (-nth entry 1))
           (loop [^not-native ret coll
                  es (seq entry)]
             (if (nil? es)
               ret
               (let [^not-native e (first es)]
                 (if (vector? e)
                   (recur
                     (-assoc ret (-nth e 0) (-nth e 1))
                     (next es))
                   (throw (js/Error. "conj on a map takes map entries or seqables of map entries"))))))))
       ])

  ITransactable
  (db-schema [this]
    schema)
  (tx-get [this]
    tx)
  (tx-begin [this]
    (when tx
      (throw (ex-info "already in tx" {})))

    (GroveDB.
      schema
      data
      (Transaction.
        data
        #{} #_(transient #{})
        #{} #_(transient #{})
        #{} #_(transient #{})
        (volatile! false))))

  ITransactableCommit
  (tx-commit! [_]
    (when-not tx
      (throw (ex-info "not in transaction" {})))
    (assoc
      (tx-commit! tx)
      :db (GroveDB. schema data nil)
      :data data)))

(defn transacted [^GroveDB db]
  (tx-begin db))

(defn observed [^GroveDB db]
  (ObservedData.
    (transient #{})
    ;; we just need the data map
    ;; checking so this can also work with regular maps
    (if (instance? GroveDB db) @db db)))


;; doesn't work with transient keys
(defn recorded-tx-keys [^GroveDB db]
  (when-some [tx (.-tx db)]
    {:keys-new     (.-keys-new tx)
     :keys-removed (.-keys-removed tx)
     :keys-updated (.-keys-updated tx)}))


(defn key-removed? [db-before db-after k]
  (and (contains? db-before k) (not (contains? db-after k))))

(defn key-added? [db-before db-after k]
  (and (not (contains? db-before k)) (contains? db-after k)))

(defn key-updated? [db-before db-after k]
  (and (contains? db-before k) (contains? db-after k)))

(defn filters [pred set]
  (reduce (fn [acc itm] (if (pred itm) acc (disj acc itm)))
          set set))

(comment
  (filters pos? #{-1 0 1 2}))

;; TODO: could be a protocol implemented by GDB?
(defn normalise-tx-keys
  "- key is truly removed if it was present in db-before but is absent in db-after
   - key is truly new if absent in db-before but present in db-after
   - keys is updated if present in both places"
  ([db-before db]
   (let [{:keys [keys-new keys-updated keys-removed]} (recorded-tx-keys db)]
     (normalise-tx-keys db-before db keys-new keys-updated keys-removed)))
  ([db-before db-after keys-new keys-updated keys-removed]
   (let [filters* (fn [f ks] (filters #(f db-before db-after %) ks))]
     {:keys-new     (filters* key-added? keys-new)
      :keys-updated (filters* key-updated? keys-updated)
      :keys-removed (filters* key-removed? keys-removed)})))


;; TODO: perhaps use a key like ::invalid-type, instead of nil
(defn default-type-fn [_]
  nil)

(defn configure
  "Returns a [[GroveDB]] instance with the associated `spec`.
   You may optionally initialize the db data to `init-db`, but **note**: init-db
   will not have ::all colls handled.

   `spec` currently only supports the optional `:type-fn`, a fn that takes the
   value being assoced into the db and returns the entity type for
   [::all entity-type] collections. Returning `nil` will not add the key to any
   :all colls.

   ---
   Example:
   ```
   (defonce data-ref
     (-> (db/configure schema)
         (atom)))
   ```"
  ([spec]
   (configure {} spec))
  ([init-db spec]
   (let [schema (cond-> spec
                  (not (fn? (:type-fn spec)))
                  (assoc :type-fn default-type-fn))]
     (GroveDB. schema init-db nil))))

(defn all-keys-of
  "Returns the set of all idents of `entity-type`."
  [db entity-type]
  (get db (coll-key entity-type)))

(defn all-of
  "Returns vals of all idents of `entity-type`."
  [db entity-type]
  (->> (all-keys-of db entity-type)
       (map #(get db %))))


(comment
  (defn xt-doc? [thing]
    (and (map? thing) (contains? thing :xt/id)))

  (defn doc-type [thing]
    (when (xt-doc? thing)
      (condp #(contains? %2 %1) thing
        :block/type :block
        :prop/name  :prop
        :enum/value :enum
        :view/query :view)))


  (let [db (configure {:type-fn doc-type})
        tx-db (transacted db)
        db' (-> (assoc tx-db "b-1" {:xt/id "b-1" :block/type ""})
                (assoc "b-1" {:xt/id "b-1" :block/type "foo"})
                (assoc "p-1" {:xt/id "p-1" :prop/name "bar"})
                (dissoc "p-1" "b-1")
                (assoc :boo {:not-a-valid-doc :boo}))
        post-db (tx-commit! db')]

    (= (dissoc post-db :db)

       {:data {:boo {:not-a-valid-doc :boo},
               [:shadow.grove.db/all :block] #{},
               [:shadow.grove.db/all :prop] #{}},
        :data-before {},
        :keys-new #{:boo "b-1" "p-1"},
        :keys-removed #{"b-1" "p-1"},
        :keys-updated #{[:shadow.grove.db/all :block] [:shadow.grove.db/all :prop]
                        "b-1"}}))


  (let [db (configure {:type-fn doc-type})
        tx-db (transacted db)
        db' (-> (assoc tx-db "b-1" {:xt/id "b-1" :block/type ""})
                (assoc "b-1" {:xt/id "b-1" :block/type "foo"})
                (assoc "p-1" {:xt/id "p-1" :prop/name "bar"})
                (dissoc "p-1" "b-1")
                (assoc :boo {:not-a-valid-doc :boo}))
        tx-db' (.-tx db')]

    (= {:keys-new (persistent! (.-keys-new tx-db'))
        :keys-updated (persistent! (.-keys-updated tx-db'))
        :keys-removed (persistent! (.-keys-removed tx-db'))}

       {:keys-new #{:boo "b-1" "p-1"},
        :keys-removed #{"b-1" "p-1"},
        :keys-updated #{[:shadow.grove.db/all :block] [:shadow.grove.db/all :prop]
                        "b-1"}})))

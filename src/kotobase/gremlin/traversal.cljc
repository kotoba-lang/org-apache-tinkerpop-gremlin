(ns kotobase.gremlin.traversal
  "gremlin.kotobase.net -- a v0.1 Apache TinkerPop (tinkerpop.apache.org)
  Gremlin traversal subset over kotobase, via the shared
  `kotobase.query.bridge` (ADR-2607172300 in `com-junkawasaki/root`,
  ADR-2607172500 scoping this repo).

  Pure `.cljc` -- **zero socket/GraphSON-framing concepts in this
  namespace**, same separation-of-concerns discipline `org-ietf-sftp`
  (`kotobase.sftp.fs` vs `kotobase.sftp.transport.*`) and
  `org-postgresql-wire` use between core query-translation logic and
  transport. The `.cljs`-only WebSocket + GraphSON wire layer that calls
  into `execute` below lives in `kotobase.gremlin.wire`.

  ## Bytecode-shaped input, not raw Gremlin script text (deliberate v0.1
  scope reduction, ADR-2607172500)

  This namespace does **not** parse Groovy-shaped Gremlin script strings
  (`g.V().hasLabel('users').values('name')` as source text) -- it accepts a
  small EDN vector-of-steps \"bytecode\" that is structurally close to how
  real TinkerPop Bytecode is shaped (a flat list of `[step-name & args]`
  instructions), just simplified to plain EDN instead of TinkerPop's
  GraphSON-typed Bytecode object graph:

  ```clojure
  [[:V] [:hasLabel \"users\"] [:has \"role\" \"admin\"] [:values \"name\"]]
  ```

  is the bytecode-shaped equivalent of the Gremlin traversal
  `g.V().hasLabel('users').has('role', 'admin').values('name')`. Avoiding a
  Groovy-subset parser on top of everything else this repo already does
  (translation + a real WebSocket transport) is an honest, deliberate v0.1
  scope boundary -- see the References section below and this repo's
  README for exactly what is and is not supported.

  ## v0.1 traversal-step subset (hard scope boundary)

  | Step | Bytecode shape | Meaning |
  |---|---|---|
  | `g.V()` | `[:V]` | scan a materialized vertex collection -- MUST be the first step |
  | `.hasLabel(label)` | `[:hasLabel label]` | filter: current vertex's `:kotobase/coll` (materialized collection name) equals `label` |
  | `.has(prop, value)` | `[:has prop value]` | filter: current vertex's `prop` attribute equals `value` (equality only) |
  | `.out(edgeLabel)` | `[:out edgeLabel]` | outgoing edge traversal -- see \"Edge convention\" below |
  | `.in(edgeLabel)` | `[:in edgeLabel]` | incoming edge traversal -- see \"Edge convention\" below |
  | `.values(prop)` | `[:values prop]` | projection, TERMINAL step -- MUST be the last step, produces the result list |

  **Explicitly OUT OF SCOPE for v0.1** (rejected with a clear error by
  `execute`/`translate`, never silently ignored or partially executed):
  full Gremlin script `eval` (arbitrary Groovy-shaped expressions --
  n/a here since input is already bytecode-shaped, not script text, but
  worth stating for anyone porting a real GraphSON `\"op\": \"eval\"`
  request into this namespace: it is not supported, only `\"op\":
  \"bytecode\"` is, see `kotobase.gremlin.wire`), `.repeat()`/variable-length
  paths, lambda/closure steps, mutation steps (`addV`/`addE`/`drop`/`property`
  writes), transactions, `.hasLabel`/`.values` with more than one argument
  (real Gremlin's multi-key `.values(k1, k2)` has flat multi-key fan-out
  stream semantics that would change this translator's single-`?var`-per-
  step shape -- out of scope, rejected explicitly), `.hasLabel`/`.has` OR
  semantics (`.hasLabel(l1, l2)`), any step name not in the table above.

  ## Vertex collections MUST be declared explicitly by the caller

  Unlike `org-opencypher-cypher`'s Cypher translator (where every node
  pattern is labeled on both ends of a relationship,
  `(a:LabelA)-[:REL]->(b:LabelB)`, so the set of collections to materialize
  is mechanically derivable from the parsed statement), a Gremlin
  `.out(edgeLabel)`/`.in(edgeLabel)` step does NOT self-declare the target
  vertex's label -- `g.V().hasLabel('users').out('worksAt').values('name')`
  never names the `departments` collection in the traversal at all. This
  namespace therefore requires the CALLER to declare, up front, every
  `kotobase.store` collection that should be materialized as vertices --
  the `:vertex-colls` key in `execute`'s `ctx` map, a seq of collection
  identifiers exactly like `kotobase.query.bridge/materialize`'s own
  `coll-keys` argument (which this namespace passes it straight through to).
  This is this repo's own, deliberate v0.1 convention (documented here, not
  silently assumed) -- the deploy shell / test fixtures that own the graph
  schema are expected to supply the full vertex-collection set, the same
  \"deploy shell provides the schema\" discipline ADR-2607172500 uses for
  `org-graphql-http`'s SDL.

  ## Edge convention: reference-valued attribute -> target `:kotobase/key`

  There is no first-class \"edge\" concept in kotobase's flat document
  model (`kotobase.store` is get/put/list per collection, nothing else) --
  same starting point `org-opencypher-cypher`'s relationship-pattern bonus
  scope documents for itself. This repo's own convention, applied
  mechanically: a materialized vertex represents an outgoing edge labeled
  `edgeLabel` by carrying an attribute named `(keyword edgeLabel)` whose
  value equals the TARGET vertex's `:kotobase/key` (a STRING, per
  `kotobase.query.bridge`'s own doc->datoms mapping -- materialized keys are
  always strings). Concretely, for `.out(\"worksAt\")` starting from a
  `users` vertex to reach a `departments` vertex, the `users` document must
  carry `{:worksAt \"d1\" ...}` where `\"d1\"` is the target department's
  key. `.in(\"worksAt\")` is the exact reverse traversal (find every vertex
  whose `:worksAt` attribute equals the CURRENT vertex's `:kotobase/key`) --
  it does not require a separately-stored 'incoming edge' list, it is a
  Datalog join run in the opposite direction over the SAME stored
  attribute. This is exactly the `fk-attr` convention
  `kotobase.protocols.cypher`'s relationship-pattern bonus scope uses,
  applied via `edgeLabel` directly (Gremlin edge labels have no
  Cypher-style `SNAKE_CASE` -> lower-case folding rule to mirror -- the
  label string is used verbatim as the attribute name, via `keyword`).

  ## Result semantics -- deduplicated set, not a Gremlin bag

  Real Gremlin traversals have *bag* (multiset) semantics -- `.values()`
  can and does emit duplicate values for duplicate matching paths.
  `kotobase.query.bridge/q` (and the `arrangement.datalog` engine
  underneath it) returns a mathematical SET of result tuples -- this
  namespace inherits that dedup behavior unchanged rather than
  reimplementing bag semantics on top, and documents it here as an honest
  v0.1 simplification (same simplification `org-opencypher-cypher`
  documents for its own `execute`, which also delegates straight to the
  bridge). `execute` sorts the (already-deduplicated) results by
  stringified value for deterministic output across runs -- there is no
  `.order()` step in v0.1, this is an implementation convenience only, not
  a Gremlin ordering guarantee.

  ## `visible?` -- required, injectable, never defaulted

  `ctx` passed to `execute` MUST include `:visible?`, a predicate over
  materialized datoms (`(fn [{:keys [s p o]}] boolean?)`) -- the same
  discipline `kotobase.query.bridge`/`arrangement.datalog`/
  `kotobase.protocols.cypher` enforce (ADR-2607050500, \"Query as
  first-class effect\"). `execute` throws immediately (a ctx-construction
  bug, not a traversal error) if `:visible?` is missing -- there is no
  silent `(constantly true)` fallback anywhere in this namespace.

  ## References

  - ADR-2607172500 (`com-junkawasaki/root`) -- the ADR that scoped this repo
  - ADR-2607172300 -- the four-surface `kotobase-query` precedent this ADR
    extends, and the source of the required, non-defaulted `visible?`
    discipline
  - `kotobase.protocols.cypher` (`kotoba-lang/org-opencypher-cypher`) -- the
    sibling translator this namespace's join convention and
    execute/translate split are modeled on"
  (:require [kotobase.query.bridge :as bridge]))

;; ------------------------------------------------------------------ errors

(defn- gremlin-err
  "An `ex-info` for any traversal-translation failure -- out-of-scope step,
  malformed bytecode, missing/duplicate terminal step, wrong arity. `code`
  is a short machine-readable keyword (mirrors `kotobase.protocols.cypher`'s
  `:cypher/code` convention, this repo's own naming); `msg` is the
  human-readable message callers (including `kotobase.gremlin.wire`) should
  surface."
  [code msg]
  (ex-info msg {:gremlin/code code :gremlin/message msg}))

(def ^:private known-steps
  "The full v0.1 step vocabulary -- anything else is rejected by `translate`
  with a clear, explicit error naming both what WAS given and the
  out-of-scope list this repo does not implement (ADR-2607172500)."
  #{:V :hasLabel :has :out :in :values})

(def ^:private out-of-scope-note
  "Explicitly OUT OF SCOPE for v0.1 (ADR-2607172500): full Gremlin script eval, .repeat()/variable-length paths, lambda/closure steps, mutation steps (addV/addE/drop/property), transactions, GraphBinary serialization, multi-key .values(k1,k2,...) fan-out, .hasLabel/.has OR-semantics.")

;; --------------------------------------------------------------- translate

(defn- step-arity-err [step-name n args]
  (throw (gremlin-err :gremlin/bad-arity
                       (str "step " (name step-name) " expects exactly " n
                            " argument" (when (not= n 1) "s") ", got "
                            (count args) ": " (pr-str args)))))

(defn- unknown-step-err [tup]
  (throw (gremlin-err :gremlin/unsupported-step
                       (str "unsupported traversal step " (pr-str tup)
                            " -- v0.1 only supports " (pr-str (vec (sort known-steps)))
                            ". " out-of-scope-note))))

(defn- var-sym [n] (symbol (str "?v" n)))

(defn- step-name [tup]
  (when-not (and (vector? tup) (seq tup) (keyword? (first tup)))
    (throw (gremlin-err :gremlin/malformed-step
                         (str "each bytecode step must be a vector starting with a keyword"
                              " step name, e.g. [:hasLabel \"users\"] -- got " (pr-str tup)))))
  (first tup))

(defn translate
  "`bytecode` (a vector of `[step-name & args]` tuples, see ns docstring) ->
  a `kotobase.query.bridge`-shaped query descriptor:
  `{:query {:find [...] :where [...]} :terminal-prop \"...\"}`.

  Throws `ex-info` (`(:gremlin/code (ex-data e))`, `(:gremlin/message
  (ex-data e))`) for anything outside the v0.1 subset -- see ns docstring's
  step table. Never silently accepts or partially executes out-of-scope
  syntax.

  `bytecode` MUST start with `[:V]` and end with exactly one `[:values
  prop]` step (the terminal/projection step) -- both requirements are this
  repo's own v0.1 convention (documented in the ns docstring), not
  something TinkerPop itself mandates for every traversal."
  [bytecode]
  (when-not (and (sequential? bytecode) (seq bytecode))
    (throw (gremlin-err :gremlin/empty-bytecode
                         "bytecode must be a non-empty vector of [step-name & args] tuples")))
  (when-not (= :V (step-name (first bytecode)))
    (throw (gremlin-err :gremlin/missing-V
                         (str "bytecode must start with [:V] (g.V()) -- got "
                              (pr-str (first bytecode))))))
  (let [last-tup (last bytecode)]
    (doseq [tup (butlast (rest bytecode))]
      (when (= :values (step-name tup))
        (throw (gremlin-err :gremlin/values-not-terminal
                             ".values(prop) is only supported as the LAST step in v0.1 -- got it mid-traversal"))))
    (when-not (= :values (step-name last-tup))
      (throw (gremlin-err :gremlin/missing-terminal-values
                           (str "bytecode must end with exactly one [:values prop] step"
                                " (the terminal projection step) -- got " (pr-str last-tup))))))
  (loop [steps (rest bytecode)
         cur-var (var-sym 0)
         next-id 1
         where []]
    (let [tup (first steps)
          [op & args] tup]
      (cond
        (nil? tup)
        (throw (gremlin-err :gremlin/missing-terminal-values
                             "bytecode must end with a [:values prop] step"))

        (= :V op)
        (throw (gremlin-err :gremlin/duplicate-V
                             "[:V] (g.V()) is only supported as the FIRST step in v0.1"))

        (= :hasLabel op)
        (do (when (not= 1 (count args)) (step-arity-err op 1 args))
            (recur (rest steps) cur-var next-id
                   (conj where [cur-var :kotobase/coll (str (first args))])))

        (= :has op)
        (do (when (not= 2 (count args)) (step-arity-err op 2 args))
            (let [[prop v] args]
              (recur (rest steps) cur-var next-id
                     (conj where [cur-var (keyword (str prop)) v]))))

        (= :out op)
        (do (when (not= 1 (count args)) (step-arity-err op 1 args))
            (let [edge-label (str (first args))
                  fk-var (symbol (str "?fk" next-id))
                  new-var (var-sym next-id)]
              (recur (rest steps) new-var (inc next-id)
                     (-> where
                         (conj [cur-var (keyword edge-label) fk-var])
                         (conj [new-var :kotobase/key fk-var])))))

        (= :in op)
        (do (when (not= 1 (count args)) (step-arity-err op 1 args))
            (let [edge-label (str (first args))
                  fk-var (symbol (str "?fk" next-id))
                  new-var (var-sym next-id)]
              (recur (rest steps) new-var (inc next-id)
                     (-> where
                         (conj [new-var (keyword edge-label) fk-var])
                         (conj [cur-var :kotobase/key fk-var])))))

        (= :values op)
        (do (when (not= 1 (count args)) (step-arity-err op 1 args))
            (let [prop (str (first args))
                  result-var (symbol (str "?r" next-id))]
              ;; :values is validated as the terminal step above -- reaching
              ;; here with steps still remaining is a translate() bug, not a
              ;; user-facing error, so no explicit check/throw is needed.
              {:query {:find [result-var]
                       :where (conj where [cur-var (keyword prop) result-var])}
               :vertex-var cur-var}))

        :else (unknown-step-err tup)))))

;; ----------------------------------------------------------------- execute

(defn execute
  "Run bytecode-shaped `bytecode` (see ns docstring) against `ctx`:
  `{:store IStore :vertex-colls [coll-key ...] :visible? (fn [datom] bool)}`.

  Translates `bytecode` (via `translate`) into a `kotobase.query.bridge`
  query, materializes `:vertex-colls` from `:store`, runs the query filtered
  by the REQUIRED `:visible?` predicate, and returns a SORTED, deduplicated
  vector of projected `.values(prop)` results (see ns docstring's \"Result
  semantics\" section for why this is a set, not a Gremlin bag).

  Throws immediately (a ctx-construction bug, not a traversal error) if
  `:visible?` is missing, or if `:vertex-colls` is missing/empty (ADR-
  2607050500 discipline, same as `kotobase.query.bridge`/
  `kotobase.protocols.cypher`)."
  [{:keys [store vertex-colls visible?]} bytecode]
  (when-not (fn? visible?)
    (throw (gremlin-err :gremlin/missing-visible
                         (str "kotobase.gremlin.traversal/execute requires ctx :visible? -- a"
                              " REQUIRED predicate fn over materialized datoms, no permissive"
                              " default (ADR-2607050500). Pass (constantly true) to see"
                              " everything materialized."))))
  (when-not (and (sequential? vertex-colls) (seq vertex-colls))
    (throw (gremlin-err :gremlin/missing-vertex-colls
                         (str "kotobase.gremlin.traversal/execute requires ctx :vertex-colls --"
                              " a REQUIRED, non-empty seq of kotobase.store collection"
                              " identifiers to materialize as the graph's vertices (see ns"
                              " docstring's \"Vertex collections MUST be declared explicitly\""
                              " section). Got: " (pr-str vertex-colls)))))
  (let [{:keys [query]} (translate bytecode)
        rows (bridge/query store vertex-colls query visible?)]
    (vec (sort-by str (map first rows)))))

(ns kotobase.gremlin.traversal-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase.local :as local]
            [kotobase.store :as st]
            [kotobase.gremlin.traversal :as traversal]))

(def ^:private everything (constantly true))

(defn- fixture-store
  "A LocalStore with two collections: `users` (the edge convention this repo
  documents -- `:worksAt` holds the TARGET department's `:kotobase/key`
  string, see `kotobase.gremlin.traversal`'s ns docstring \"Edge convention\"
  section) and `departments` (the join target). One user (Dave) has no
  `:worksAt` at all, mirroring `kotobase-query`'s own bridge_test fixture
  convention for an unbound-join case."
  []
  (let [s (local/local-store)]
    (st/-put s "users" "u1" {:name "Alice" :role "admin" :worksAt "d1"})
    (st/-put s "users" "u2" {:name "Bob" :role "user" :worksAt "d2"})
    (st/-put s "users" "u3" {:name "Carol" :role "admin" :worksAt "d1"})
    (st/-put s "users" "u4" {:name "Dave" :role "user"}) ; no :worksAt
    (st/-put s "departments" "d1" {:name "Engineering" :budget 900000})
    (st/-put s "departments" "d2" {:name "Sales" :budget 400000})
    s))

;; ---------------------------------------------------------------- translate

(deftest translate-rejects-bytecode-not-starting-with-V
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"must start with \[:V\]"
       (traversal/translate [[:hasLabel "users"] [:values "name"]]))))

(deftest translate-rejects-bytecode-not-ending-with-values
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"must end with exactly one \[:values"
       (traversal/translate [[:V] [:hasLabel "users"]]))))

(deftest translate-rejects-values-mid-traversal
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"only supported as the LAST step"
       (traversal/translate [[:V] [:values "name"] [:hasLabel "users"]]))))

;; ------------------------------------------------------------------ execute

(deftest v-hasLabel-values-chain
  (testing "g.V().hasLabel('users').values('name') -- scan + label filter + projection"
    (let [result (traversal/execute
                  {:store (fixture-store) :vertex-colls ["users"] :visible? everything}
                  [[:V] [:hasLabel "users"] [:values "name"]])]
      (is (= ["Alice" "Bob" "Carol" "Dave"] result)))))

(deftest v-hasLabel-has-values-chain
  (testing "g.V().hasLabel('users').has('role', 'admin').values('name') -- equality filter"
    (let [result (traversal/execute
                  {:store (fixture-store) :vertex-colls ["users"] :visible? everything}
                  [[:V] [:hasLabel "users"] [:has "role" "admin"] [:values "name"]])]
      (is (= ["Alice" "Carol"] result)))))

(deftest v-has-out-values-chain-real-cross-collection-join
  (testing "g.V().hasLabel('users').has('role', 'admin').out('worksAt').values('name')
    -- exercises the real kotobase-query cross-collection join (users -> departments)"
    (let [result (traversal/execute
                  {:store (fixture-store) :vertex-colls ["users" "departments"] :visible? everything}
                  [[:V] [:hasLabel "users"] [:has "role" "admin"] [:out "worksAt"] [:values "name"]])]
      (is (= ["Engineering"] result)
          "both Alice and Carol (admins) work at Engineering -- deduplicated to one row,
          see ns docstring's \"Result semantics\" section"))))

(deftest v-out-values-drops-unbound-edge
  (testing "Dave has no :worksAt -- the join clause fails to match, he drops out entirely
    (not a nil row), same discipline kotobase-query's own join test documents"
    (let [result (traversal/execute
                  {:store (fixture-store) :vertex-colls ["users" "departments"] :visible? everything}
                  [[:V] [:hasLabel "users"] [:values "name"]])]
      (is (some #{"Dave"} result) "Dave IS present without a join"))
    (let [result (traversal/execute
                  {:store (fixture-store) :vertex-colls ["users" "departments"] :visible? everything}
                  [[:V] [:hasLabel "users"] [:out "worksAt"] [:values "name"]])]
      (is (not (some #{"Dave"} result)) "but Dave never appears via a .out('worksAt') traversal"))))

(deftest v-in-values-chain-reverse-join
  (testing "g.V().hasLabel('departments').has('name', 'Engineering').in('worksAt').values('name')
    -- the reverse traversal: departments -> users who work there"
    (let [result (traversal/execute
                  {:store (fixture-store) :vertex-colls ["users" "departments"] :visible? everything}
                  [[:V] [:hasLabel "departments"] [:has "name" "Engineering"]
                   [:in "worksAt"] [:values "name"]])]
      (is (= ["Alice" "Carol"] result)))))

(deftest visible-filters-out-redacted-entities
  (let [no-bob? (fn [{:keys [s]}] (not= s :users/u2))
        result (traversal/execute
                {:store (fixture-store) :vertex-colls ["users"] :visible? no-bob?}
                [[:V] [:hasLabel "users"] [:values "name"]])]
    (is (not (some #{"Bob"} result)))
    (is (some #{"Alice"} result))))

(deftest visible-is-required-not-defaulted
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"REQUIRED"
       (traversal/execute {:store (fixture-store) :vertex-colls ["users"]}
                           [[:V] [:hasLabel "users"] [:values "name"]]))
      "execute refuses to run with no stated visibility decision, same discipline
      as kotobase.query.bridge/q (ADR-2607050500)"))

(deftest vertex-colls-is-required-not-defaulted
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"REQUIRED"
       (traversal/execute {:store (fixture-store) :visible? everything}
                           [[:V] [:hasLabel "users"] [:values "name"]]))
      "execute refuses to run with no declared vertex collections -- Gremlin's
      .out()/.in() steps never self-declare a target label the way Cypher's
      relationship patterns do, see ns docstring"))

;; ---------------------------------------------------- out-of-scope rejection

(deftest rejects-unsupported-step-with-clear-error
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"unsupported traversal step"
       (traversal/execute
        {:store (fixture-store) :vertex-colls ["users"] :visible? everything}
        [[:V] [:repeat [:out "worksAt"]] [:values "name"]]))
      ".repeat() (variable-length paths) is explicitly out of scope for v0.1 (ADR-2607172500)"))

(deftest rejects-addV-mutation-step
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
       #"unsupported traversal step"
       (traversal/execute
        {:store (fixture-store) :vertex-colls ["users"] :visible? everything}
        [[:V] [:addV "users"] [:values "name"]]))
      "addV/addE/drop mutation steps are explicitly out of scope for v0.1 (ADR-2607172500)"))

(deftest rejects-eval-shaped-input-is-not-representable-as-bytecode
  (testing "there is no [:eval \"...\"] step at all -- full Gremlin script eval is out of
    scope for v0.1 and this translator has no code path that would accept a raw script
    string in the first place (see kotobase.gremlin.wire for the wire-level 'op' check)"
    (is (thrown-with-msg?
         #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
         #"unsupported traversal step"
         (traversal/execute
          {:store (fixture-store) :vertex-colls ["users"] :visible? everything}
          [[:V] [:eval "g.V().values('name')"] [:values "name"]])))))

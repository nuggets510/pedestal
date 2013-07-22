; Copyright 2013 Relevance, Inc.

; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0)
; which can be found in the file epl-v10.html at the root of this distribution.
;
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
;
; You must not remove this notice, or any other, from this software.

(ns io.pedestal.app.test.async-dataflow
  (:require [io.pedestal.app.messages :as msg]
            [io.pedestal.app.dataflow :as df])
  (:use [clojure.core.async :only [chan close! >!! <!! >! <! go alts!! thread timeout]]
        io.pedestal.app.async-dataflow
        io.pedestal.app.util.test
        clojure.test))

(refer-privates io.pedestal.app.async-dataflow :all)

(deftest test-build
  (is (= (dissoc (build {:transform [[:inc [:a] 'x]
                                     {:key :dec :out [:b] :fn 'y}]})
                 :in :out)
         {:input-adapter identity
          :transform [{:key :inc :out [:a] :fn 'x}
                      {:key :dec :out [:b] :fn 'y}]
          :derive ()
          :continue #{}
          :effect #{}
          :emit []}))
  (is (= (dissoc (build {:derive [{:in #{[:a]} :out [:b] :fn 'b}
                                  {:in #{[:b]} :out [:c] :fn 'c}]})
                 :in :out)
         {:input-adapter identity
          :derive [{:fn 'b :in #{[:a]} :out [:b] :arg-names nil}
                   {:fn 'c :in #{[:b]} :out [:c] :arg-names nil}]
          :transform []
          :continue #{}
          :effect #{}
          :emit []}))
  (is (= (dissoc (build {:transform [[:inc [:a] 'x]
                                     {:key :dec :out [:b] :fn 'y}]
                         :derive [{:in #{[:a]} :out [:b] :fn 'b}
                                  [#{[:b]} [:c] 'c]]})
                 :in :out)
         {:input-adapter identity
          :transform [{:key :inc :out [:a] :fn 'x}
                      {:key :dec :out [:b] :fn 'y}]
          :derive [{:fn 'b :in #{[:a]} :out [:b] :arg-names nil}
                   {:fn 'c :in #{[:b]} :out [:c] :args nil :arg-names nil}]
          :continue #{}
          :effect #{}
          :emit []}))
  (is (= (dissoc (build {:transform [[:inc [:a] 'a]
                                     {:key :dec :out [:b] :fn 'b}]
                         :derive [{:in #{[:a]} :out [:b] :fn 'c}
                                  [#{[:b]} [:c] 'd]]
                         :continue #{{:in #{[:x]} :fn 'e}
                                     [#{[:y]} 'f]}
                         :effect #{{:in #{[:w]} :fn 'g}
                                   [#{[:p]} 'h]}
                         :emit [{:in #{[:q]} :fn 'i}
                                [#{[:s]} 'j]]})
                 :in :out)
         {:input-adapter identity
          :transform [{:key :inc :out [:a] :fn 'a}
                      {:key :dec :out [:b] :fn 'b}]
          :derive [{:fn 'c :in #{[:a]} :out [:b] :arg-names nil}
                   {:fn 'd :in #{[:b]} :out [:c] :args nil :arg-names nil}]
          :continue #{{:in #{[:x]} :fn 'e :arg-names nil}
                      {:in #{[:y]} :fn 'f :args nil :arg-names nil}}
          :effect #{{:in #{[:w]} :fn 'g :arg-names nil}
                    {:in #{[:p]} :fn 'h :args nil :arg-names nil}}
          :emit [{:in #{[:q]} :fn 'i :arg-names nil}
                 {:in #{[:s]} :fn 'j :args nil :arg-names nil}]})))

(deftest test-transform-phase
  (let [inc-fn (fn [o _] (inc o))
        state {:old {:data-model {:a 0}}
               :new {:data-model {:a 0}}
               :dataflow {:input-adapter identity
                          :transform [{:key :inc :out [:a] :fn inc-fn}]}
               :context {:message {:out [:a] :key :inc}}}
        cin (chan)
        cout (transform-phase cin)]
    (go (>! cin state))
    (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
           (-> state
               (assoc-in [:change :updated] #{[:a]})
               (assoc-in [:new :data-model :a] 1))))))

(deftest test-derive-phase
  (let [double-sum-fn (fn [_ input] (* 2 (reduce + (df/input-vals input))))]
    (let [state {:change {:updated #{[:a]}}
                 :old {:data-model {:a 0}}
                 :new {:data-model {:a 2}}
                 :dataflow {:derive [{:fn double-sum-fn :in (df/with-propagator #{[:a]}) :out [:b]}]}
                 :context {}}
          cin (chan)
          cout (derive-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
             (-> state
                 (assoc-in [:change :added] #{[:b]})
                 (assoc-in [:new :data-model :b] 4)))))
    (let [state {:change {:updated #{[:a]}}
                 :old {:data-model {:a 0}}
                 :new {:data-model {:a 2}}
                 :dataflow {:derive [{:fn double-sum-fn :in (df/with-propagator #{[:a]}) :out [:b]}
                                     {:fn double-sum-fn :in (df/with-propagator #{[:a]}) :out [:c]}]}
                 :context {}}
          cin (chan)
          cout (derive-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
             (-> state
                 (assoc-in [:change :added] #{[:b] [:c]})
                 (update-in [:new :data-model] assoc :b 4 :c 4)))))
    (testing "returned maps record change"
      (let [d (fn [_ input] {:x {:y 11}})
            state {:change {:updated #{[:a]}}
                   :old {:data-model {:a 0 :b {:c {:x {:y 10 :z 15}}}}}
                   :new {:data-model {:a 2 :b {:c {:x {:y 10 :z 15}}}}}
                   :dataflow {:derive [{:fn d :in (df/with-propagator #{[:a]}) :out [:b :c]}]}
                   :context {}}
            cin (chan)
            cout (derive-phase cin (:dataflow state))]
        (go (>! cin state))
        (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
               (merge state
                      {:change {:updated #{[:a] [:b :c :x :y]} :removed #{[:b :c :x :z]}}
                       :new {:data-model {:a 2 :b {:c {:x {:y 11}}}}}})))))))

(deftest test-continue-phase
  (let [continue-fn (fn [input] [{msg/topic :x msg/type :y :value (df/single-val input)}])
        state {:change {:updated #{[:a]}}
               :old {:data-model {:a 0}}
               :new {:data-model {:a 2}}
               :dataflow {:continue #{{:fn continue-fn :in (df/with-propagator #{[:a]})}}}
               :context {}}
        cin (chan)
        cout (continue-phase cin (:dataflow state))]
    (go (>! cin state))
    (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
           (assoc-in state [:new :continue] [{msg/topic :x msg/type :y :value 2}])))))

(deftest test-effect-phase
  (let [output-fn (fn [input] [{msg/topic :x msg/type :y :value (df/single-val input)}])
        state {:change {:updated #{[:a]}}
               :old {:data-model {:a 0}}
               :new {:data-model {:a 2}}
               :dataflow {:effect #{{:fn output-fn :in (df/with-propagator #{[:a]})}}}
               :context {}}
        cin (chan)
        cout (effect-phase cin (:dataflow state))]
    (go (>! cin state))
    (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
           (assoc-in state [:new :effect] [{msg/topic :x msg/type :y :value 2}])))))

(deftest test-emit-phase
  (let [emit-fn (fn [k] (fn [input] [{k {:inputs (df/input-map input)
                                       :updates (df/updated-map input)}}]))
        emit (fn [x] (set (get-in x [:new :emit])))
        state {:old {:data-model {:a 0
                                  :b 11
                                  :c {1 {:x 6 :y 5 :z 2}
                                      2 {:x 4 :y 7 :z 3}
                                      3 {:x 8 :y 1 :z 4}}}}
               :new {:data-model {:a 0
                                  :b 11
                                  :c {1 {:x 6 :y 5 :z 2}
                                      2 {:x 4 :y 7 :z 3}
                                      3 {:x 8 :y 1 :z 4}}}}
               :dataflow {:emit [{:in (df/with-propagator #{[:a]})       :fn (emit-fn :one)}
                                 {:in (df/with-propagator #{[:b]})       :fn (emit-fn :two)}
                                 {:in (df/with-propagator #{[:c :* :x]}) :fn (emit-fn :three)}
                                 {:in (df/with-propagator #{[:c :* :y]}) :fn (emit-fn :four)}
                                 {:in (df/with-propagator #{[:c :*]})    :fn (emit-fn :five)}
                                 {:in (df/with-propagator #{[:*]})       :fn (emit-fn :six)}]}
               :context {}}
        cin (chan)
        cout (emit-phase cin (:dataflow state))]
    (go (>! cin state))
    (is (= (first (<!! (go (alts! [(timeout 1000) cout]))))
           state))
    (let [state (-> state
                    (assoc-in [:new :data-model :a] 1)
                    (assoc :change {:updated #{[:a]}})
                    (assoc-in [:dataflow :emit] [{:in (df/with-propagator #{[:*]})
                                                  :fn (emit-fn :six)}]))
          cin (chan)
          cout (emit-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (emit (first (<!! (go (alts! [(timeout 1000) cout])))))
             #{{:six {:inputs {[:a] 1
                               [:b] 11
                               [:c] {1 {:x 6 :y 5 :z 2}
                                     2 {:x 4 :y 7 :z 3}
                                     3 {:x 8 :y 1 :z 4}}}
                      :updates {[:a] 1}}}})))
    (let [state (-> state
                    (assoc :change {:updated #{[:a]}})
                    (assoc-in [:new :data-model :a] 1))
          cin (chan)
          cout (emit-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (emit (first (<!! (go (alts! [(timeout 1000) cout])))))
             #{{:one {:inputs {[:a] 1}
                      :updates {[:a] 1}}}})))
    (let [state (-> state
                    (assoc :change {:updated #{[:c 1 :z]}})
                    (assoc-in [:new :data-model :c 1 :z] 9))
          cin (chan)
          cout (emit-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (emit (first (<!! (go (alts! [(timeout 1000) cout])))))
             #{{:five {:inputs {[:c 1] {:x 6 :y 5 :z 9}
                                [:c 2] {:x 4 :y 7 :z 3}
                                [:c 3] {:x 8 :y 1 :z 4}}
                       :updates {[:c 1 :z] 9}}}
               {:six {:inputs {[:a] 0
                               [:b] 11
                               [:c] {1 {:x 6 :y 5 :z 9}
                                     2 {:x 4 :y 7 :z 3}
                                     3 {:x 8 :y 1 :z 4}}}
                      :updates {[:c 1 :z] 9}}}})))
    (let [state (-> state
                    (assoc :change {:updated #{[:c 1 :x]}})
                    (assoc-in [:new :data-model :c 1 :x] 9))
          cin (chan)
          cout (emit-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (emit (first (<!! (go (alts! [(timeout 1000) cout])))))
             #{{:three {:inputs {[:c 1 :x] 9
                                 [:c 2 :x] 4
                                 [:c 3 :x] 8}
                        :updates {[:c 1 :x] 9}}}})))
    (let [state (-> state
                    (assoc :change {:updated #{[:c 1 :x] [:c 2 :y]}})
                    (assoc-in [:new :data-model :c 1 :x] 9)
                    (assoc-in [:new :data-model :c 2 :y] 15))
          cin (chan)
          cout (emit-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (emit (first (<!! (go (alts! [(timeout 1000) cout])))))
             #{{:three {:inputs {[:c 1 :x] 9
                                 [:c 2 :x] 4
                                 [:c 3 :x] 8}
                        :updates {[:c 1 :x] 9}}}
               {:four {:inputs {[:c 1 :y] 5
                                [:c 2 :y] 15
                                [:c 3 :y] 1}
                       :updates {[:c 2 :y] 15}}}})))
    (let [state (-> state
                    (assoc :change {:updated #{[:a] [:c 1 :y] [:c 2 :z]}})
                    (assoc-in [:new :data-model :a] 21)
                    (assoc-in [:new :data-model :c 1 :y] 22)
                    (assoc-in [:new :data-model :c 2 :z] 23))
          cin (chan)
          cout (emit-phase cin (:dataflow state))]
      (go (>! cin state))
      (is (= (emit (first (<!! (go (alts! [(timeout 1000) cout])))))
             #{{:one {:inputs {[:a] 21}
                      :updates {[:a] 21}}}
               {:four {:inputs {[:c 1 :y] 22
                                [:c 2 :y] 7
                                [:c 3 :y] 1}
                       :updates {[:c 1 :y] 22}}}
               {:five {:inputs {[:c 1] {:x 6 :y 22 :z 2}
                                [:c 2] {:x 4 :y 7 :z 23}
                                [:c 3] {:x 8 :y 1 :z 4}}
                       :updates {[:c 2 :z] 23}}}
               {:six {:inputs {[:a] 21
                               [:b] 11
                               [:c] {1 {:x 6 :y 22 :z 2}
                                     2 {:x 4 :y 7 :z 23}
                                     3 {:x 8 :y 1 :z 4}}}
                      :updates {[:c 2 :z] 23}}}})))))


;; Complete dataflow tests
;; ================================================================================

(defn inc-t [old-value message]
  (inc old-value))

(defn- go-async [t f]
  (let [cout (chan)]
    (go (<! (timeout t))
        (let [v (f)]
          (if v
            (>! cout v)
            (close! cout))))
    cout))

(defn inc-t-async [old-value message]
  (go-async 500 #(inc old-value)))

(defn sum-d [_ input]
  (reduce + (df/input-vals input)))

(defn sum-d-async [_ input]
  (go-async 100 #(reduce + (df/input-vals input))))

(defn double-d [_ input]
  (* 2 (df/single-val input)))

(defn double-d-async [_ input]
  (go-async 200 #(* 2 (df/single-val input))))

(defn input-maps-async [input]
  (go-async 300 #((comp vector df/input-map) input)))

(defn min-c [n]
  (fn [input]
    (when (< (df/single-val input) n)
      [{::msg/topic [:a] ::msg/type :inc}])))

(defn min-c-async [n]
  (fn [input]
    (go-async 50 #(when (< (df/single-val input) n)
                    [{::msg/topic [:a] ::msg/type :inc}]))))

(def flows
  {:one-transform {:transform [[:inc [:a] inc-t]]}
   :one-derive     {:transform [[:inc [:a] inc-t]]
                    :derive #{[#{[:a]} [:b] double-d]}}
   
   :identity       {:transform [[:id [:a] (fn [old-value message] old-value)]]
                    :effect #{{:fn (comp vector df/input-map)
                               :in (df/with-propagator #{[:a]} (constantly true))}}}
   
   :continue-to-10 {:input-adapter (fn [m] {:out (msg/topic m) :key (msg/type m)})
                    :transform [{:key :inc :out [:a] :fn inc-t}]
                    :derive #{{:fn double-d :in #{[:a]} :out [:b]}}
                    :continue #{{:fn (min-c 10) :in #{[:b]}}}}

   :everything {:input-adapter (fn [m] {:out (msg/topic m) :key (msg/type m)})
                :transform [{:out [:a] :key :inc :fn inc-t}]
                :derive    #{{:fn double-d :in #{[:a]} :out [:b]}
                             {:fn sum-d :in #{[:a]} :out [:c]}
                             {:fn sum-d :in #{[:b] [:c]} :out [:d]}}
                :continue  #{{:fn (min-c 10) :in #{[:d]}}}
                :effect    #{{:fn (comp vector df/input-map) :in #{[:d]}}}
                :emit      [[#{[:d]} (comp vector df/input-vals)]]}

   :everything-async {:input-adapter (fn [m] {:out (msg/topic m) :key (msg/type m)})
                      :transform [{:out [:a] :key :inc :fn inc-t-async}]
                      :derive    #{{:fn double-d-async :in #{[:a]} :out [:b]}
                                   {:fn sum-d-async :in #{[:a]} :out [:c]}
                                   {:fn sum-d :in #{[:b] [:c]} :out [:d]}}
                      :continue  #{{:fn (min-c-async 10) :in #{[:d]}}}
                      :effect    #{{:fn input-maps-async :in #{[:d]}}}
                      :emit      [[#{[:d]} (comp vector df/input-vals)]]}
   
   :always-emit {:input-adapter (fn [m] {:out (msg/topic m) :key (msg/type m)})
                 :transform [{:key :inc :fn inc-t         :out [:a]}]
                 :derive    #{{:fn double-d :in #{[:a]}   :out [:b]}
                              {:fn sum-d :in #{[:a]}      :out [:c]}
                              {:fn sum-d :in #{[:b] [:c]} :out [:d]}}
                 :continue  #{{:fn (min-c 10) :in #{[:d]}}}
                 :effect    #{{:fn (comp vector df/input-map) :in #{[:d]}}}
                 :emit      [{:in #{[:a]} :fn (fn [i] [[:always1 (df/input-map i)]]) :mode :always}
                             {:in #{[:a]} :fn (fn [i] [[:always2 (df/input-map i)]]) :mode :always}
                             [#{[:d] [:a]} (fn [i] [[:order3 (df/input-map i)]])]
                             [#{[:a]} (fn [i] [[:order4 (df/input-map i)]])]]}})

(defn flow [k]
  (build (k flows)))

(deftest test-flow-phases-step
  
  (let [state {:old {:data-model {:a 1}}
               :new {:data-model {:a 1}}
               :dataflow (flow :one-derive)
               :context {:message {:out [:a] :key :inc}}}
        cin (chan)
        cout (flow-phases-step cin (:dataflow state))]
    (go (>! cin [state {:out [:a] :key :inc}]))
    (is (= (:new (ffirst (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 2 :b 4}})))
  
  (let [state {:new {:data-model {:a 0}}
               :old {:data-model {:a 0}}
               :dataflow (flow :continue-to-10)
               :context {:message {::msg/topic [:a] ::msg/type :inc}}}
        cin (chan)
        cout (flow-phases-step cin (:dataflow state))]
    (go (>! cin [state {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (:new (ffirst (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 1 :b 2}
            :continue [{::msg/topic [:a] ::msg/type :inc}]})))

  (let [state {:new {:data-model {:a 0}}
               :old {:data-model {:a 0}}
               :dataflow (flow :everything)
               :context {:message {msg/topic [:a] msg/type :inc}}}
        cin (chan)
        cout (flow-phases-step cin (:dataflow state))]
    (go (>! cin [state {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (:new (ffirst (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 1 :b 2 :c 1 :d 3}
            :continue [{msg/topic [:a] msg/type :inc}]})))

  (let [state {:new {:data-model {:a 0}}
               :old {:data-model {:a 0}}
               :dataflow (flow :everything-async)
               :context {:message {msg/topic [:a] msg/type :inc}}}
        cin (chan)
        cout (flow-phases-step cin (:dataflow state))]
    (go (>! cin [state {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (:new (ffirst (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 1 :b 2 :c 1 :d 3}
            :continue [{msg/topic [:a] msg/type :inc}]}))))

(deftest test-run-flow-phases
  
  (let [state {:old {:data-model {:a 0}}
               :new {:data-model {:a 0}}
               :dataflow (flow :one-derive)
               :context {:message {:out [:a] :key :inc}}}
        cin (chan)
        cout (run-flow-phases cin (:dataflow state))]
    (go (>! cin [state {:out [:a] :key :inc}]))
    (is (= (:new (first (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 1 :b 2}})))
  
  (let [state {:new {:data-model {:a 0}}
               :old {:data-model {:a 0}}
               :dataflow (flow :continue-to-10)
               :context {:message {::msg/topic [:a] ::msg/type :inc}}}
        cin (chan)
        cout (run-flow-phases cin (:dataflow state))]
    (go (>! cin [state {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (:new (first (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 5 :b 10}})))

  (let [state {:new {:data-model {:a 0}}
               :old {:data-model {:a 0}}
               :dataflow (flow :everything)
               :context {:message {::msg/topic [:a] ::msg/type :inc}}}
        cin (chan)
        cout (run-flow-phases cin (:dataflow state))]
    (go (>! cin [state {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (:new (first (<!! (go (alts! [(timeout 1000) cout])))))
           {:data-model {:a 4 :b 8 :c 4 :d 12}})))

  (let [state {:new {:data-model {:a 0}}
               :old {:data-model {:a 0}}
               :dataflow (flow :everything-async)
               :context {:message {::msg/topic [:a] ::msg/type :inc}}}
        cin (chan)
        cout (run-flow-phases cin (:dataflow state))]
    (go (>! cin [state {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (:new (first (<!! (go (alts! [(timeout 10000) cout])))))
           {:data-model {:a 4 :b 8 :c 4 :d 12}}))))

(deftest test-run
  
  (let [test-flow (flow :one-transform)]
    (go (>! (:in test-flow) [{:data-model {:a 0}} {:out [:a] :key :inc}]))
    (is (= (<!! (go (alts! [(timeout 1000) (:out test-flow)])))
           [{:data-model {:a 1}} (:out test-flow)])))
  
  (let [test-flow (flow :one-derive)]
    (go (>! (:in test-flow) [{:data-model {:a 0}} {:out [:a] :key :inc}]))
    (is (= (<!! (go (alts! [(timeout 1000) (:out test-flow)])))
           [{:data-model {:a 1 :b 2}} (:out test-flow)])))

  (let [test-flow (flow :continue-to-10)]
    (go (>! (:in test-flow) [{:data-model {:a 0}} {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (<!! (go (alts! [(timeout 1000) (:out test-flow)])))
           [{:data-model {:a 5 :b 10}} (:out test-flow)])))

  (testing "custom propagator works"
    (let [test-flow (flow :identity)]
      (go (>! (:in test-flow) [{:data-model {:a 1}} {:out [:a] :key :id}]))
      (is (= (<!! (go (alts! [(timeout 1000) (:out test-flow)])))
             [{:data-model {:a 1} :effect [{[:a] 1}]} (:out test-flow)]))))

  (let [test-flow (flow :everything)]
    (go (>! (:in test-flow) [{:data-model {:a 0}} {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (<!! (go (alts! [(timeout 1000) (:out test-flow)])))
           [{:data-model {:a 4 :b 8 :c 4 :d 12}
             :effect [{[:d] 12}]
             :emit [[12]]}
            (:out test-flow)])))

  (let [test-flow (flow :everything-async)]
    (go (>! (:in test-flow) [{:data-model {:a 0}} {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (<!! (go (alts! [(timeout 10000) (:out test-flow)])))
           [{:data-model {:a 4 :b 8 :c 4 :d 12}
             :effect [{[:d] 12}]
             :emit [[12]]}
            (:out test-flow)])))

  (let [test-flow (flow :always-emit)]
    (go (>! (:in test-flow) [{:data-model {:a 0}} {::msg/topic [:a] ::msg/type :inc}]))
    (is (= (<!! (go (alts! [(timeout 1000) (:out test-flow)])))
           [{:data-model {:a 4 :b 8 :c 4 :d 12}
             :effect [{[:d] 12}]
             :emit [[:always1 {[:a] 4}]
                    [:always2 {[:a] 4}]
                    [:order3 {[:a] 4 [:d] 12}]]}
            (:out test-flow)]))))


;; Ported tests
;; ================================================================================

(deftest test-dataflows
  (let [dataflow (build {:transform [[:inc [:x] inc-t]]
                         :derive    #{{:fn sum-d :in #{[:x]}      :out [:a]}
                                      {:fn sum-d :in #{[:x] [:a]} :out [:b]}
                                      {:fn sum-d :in #{[:b]}      :out [:c]}
                                      {:fn sum-d :in #{[:a]}      :out [:d]}
                                      {:fn sum-d :in #{[:c] [:d]} :out [:e]}}})]
    (go (>! (:in dataflow) [{:data-model {:x 0}} {:out [:x] :key :inc}]))
    (is (= (first (<!! (go (alts! [(timeout 1000) (:out dataflow)]))))
           {:data-model {:x 1 :a 1 :b 2 :d 1 :c 2 :e 3}})))

  (let [dataflow (build {:transform [[:inc [:x] inc-t]]
                         :derive    #{{:fn sum-d :in #{[:x]}           :out [:a]}
                                      {:fn sum-d :in #{[:a]}           :out [:b]}
                                      {:fn sum-d :in #{[:a]}           :out [:c]}
                                      {:fn sum-d :in #{[:c]}           :out [:d]}
                                      {:fn sum-d :in #{[:c]}           :out [:e]}
                                      {:fn sum-d :in #{[:d] [:e]}      :out [:f]}
                                      {:fn sum-d :in #{[:a] [:b] [:f]} :out [:g]}
                                      {:fn sum-d :in #{[:g]}           :out [:h]}
                                      {:fn sum-d :in #{[:g] [:f]}      :out [:i]}
                                      {:fn sum-d :in #{[:i] [:f]}      :out [:j]}
                                      {:fn sum-d :in #{[:h] [:g] [:j]} :out [:k]}}})]
    (go (>! (:in dataflow) [{:data-model {:x 0}} {:out [:x] :key :inc}]))
    (is (= (first (<!! (go (alts! [(timeout 1000) (:out dataflow)]))))
           {:data-model {:x 1 :a 1 :b 1 :c 1 :d 1 :e 1 :f 2 :g 4 :h 4 :i 6 :j 8 :k 16}})))

  (let [dataflow (build {:transform [[:inc [:x :* :y :* :b] inc-t]]
                         :derive    [{:fn sum-d :in #{[:x :* :y :* :b]} :out [:sum :b]}]})]
    (go (>! (:in dataflow) [{:data-model {:x {0 {:y {0 {:a 1
                                                        :b 5}}}
                                              1 {:y {0 {:a 1}
                                                     1 {:b 2}}}}
                                          :sum {:b 7
                                                :a 2}}}
                            {:out [:x 1 :y 1 :b] :key :inc}]))
    (is (= (first (<!! (go (alts! [(timeout 1000) (:out dataflow)]))))
           {:data-model {:x {0 {:y {0 {:a 1
                                       :b 5}}}
                             1 {:y {0 {:a 1}
                                    1 {:b 3}}}}
                         :sum {:a 2
                               :b 8}}}))))

(deftest test-multiple-deep-changes
  (let [results (atom nil)
        t (fn [state message]
            (-> state
                (update-in [:c] (fnil inc 0))
                (update-in [:d] (fnil inc 0))))
        e (fn [inputs]
            (reset! results inputs)
            [])
        dataflow (build {:transform [[:a [:b] t]]
                         :emit [[#{[:* :*]} e]]})]
    (go (>! (:in dataflow) [{:data-model {:b {}}} {:key :a :out [:b]}]))
    (is (= (first (<!! (go (alts! [(timeout 1000) (:out dataflow)]))))
           {:data-model {:b {:c 1 :d 1}}
            :emit []}))
    (is (= @results
           {:added #{[:b :c] [:b :d]}
            :input-paths #{[:* :*]}
            :message {:key :a, :out [:b]}
            :new-model {:b {:c 1, :d 1}}
            :old-model {:b {}}
            :removed #{}
            :updated #{}
            :mode nil
            :processed-inputs nil}))))

; Copyright 2013 Relevance, Inc.

; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0)
; which can be found in the file epl-v10.html at the root of this distribution.
;
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
;
; You must not remove this notice, or any other, from this software.

(ns io.pedestal.app.async-dataflow
  (:require [clojure.core.async.impl.channels :as channels]
            [io.pedestal.app.data.tracking-map :as tm]
            [io.pedestal.app.util.platform :as platform]
            [io.pedestal.app.dataflow :as dataflow])
  (:use [clojure.core.async :only [chan close! >!! <!! >! <! go alts!! thread timeout]]))

(defn channel? [c]
  (satisfies? channels/MMC c))

(defn- channel-fn [f]
  (let [cin (chan)
        cout (chan)]
    (go (while true
          (let [value (<! cin)
                new-value (f value)]
            (if (channel? new-value)
              (>! cout (<! new-value))
              (>! cout new-value)))))
    {:in cin :out cout}))

(defn- run-pipeline [cin fns]
  (let [cout (chan)]
    (go (while true
          (loop [fns fns
                 state (<! cin)]
            (if (seq fns)
              (let [{:keys [in out]} (first fns)]
                (>! in state)
                (recur (rest fns) (<! out)))
              (>! cout state)))))
    cout))

(defn- maybe-channel [val f]
  (if (channel? val)
    (let [cout (chan)]
      (go (let [v (<! val)]
            (>! cout (f v))))
      cout)
    (f val)))

(defn- track-update-in [data-model out-path f & args]
  (let [data-model (tm/tracking-map data-model)
        subtree (get-in data-model out-path)]
    (maybe-channel (apply f subtree args) #(assoc-in data-model out-path %))))

(defn apply-in [state out-path f & args]
  (let [data-model (get-in state [:new :data-model])
        new-data-model (apply track-update-in data-model out-path f args)]
    (maybe-channel new-data-model #(dataflow/update-flow-state state %))))

(defn- emit-fn [emit]
  (let [{input-paths :in emit-fn :fn mode :mode} emit]
    (fn [{:keys [change context remaining-change processed-inputs] :as state}]
      (let [report-change (if (= mode :always) change remaining-change)]
        (if (dataflow/propagate? (assoc state :change report-change) input-paths)
          (-> state
              (update-in [:remaining-change] dataflow/remove-matching-changes input-paths)
              (update-in [:processed-inputs] (fnil into []) input-paths)
              (update-in [:new :emit] (fnil into [])
                         (emit-fn (-> (dataflow/flow-input context state input-paths report-change)
                                      (assoc :mode mode :processed-inputs processed-inputs)))))
          state)))))

(defn- emit-phase
  [cin dataflow]
  (let [cout (chan)
        emit-input (chan)
        emit-output (run-pipeline emit-input (map #(channel-fn (emit-fn %)) (:emit dataflow)))]
    (go (while true
          (let [state (<! cin)]
            (>! emit-input (assoc state :remaining-change (:change state)))
            (>! cout (dissoc (<! emit-output) :remaining-change)))))
    cout))

(defn- output-fn [k output]
  (let [{f :fn input-paths :in args :args arg-names :arg-names} output]
    (fn [{:keys [change context] :as state}]
      (if (dataflow/propagate? state input-paths)
        (let [out-value (apply f (dataflow/dataflow-fn-args
                                  (dataflow/flow-input context state input-paths change)
                                  args
                                  arg-names))]
          (maybe-channel out-value #(update-in state [:new k] (fnil into []) %)))
        state))))

(defn- output-phase
  "Execute each function. Return an updated flow state."
  [cin dataflow k]
  (run-pipeline cin (map #(channel-fn (output-fn k %)) (k dataflow))))

(defn- continue-phase
  "Execute each continue function. Return an updated flow state."
  [cin dataflow]
  (output-phase cin dataflow :continue))

(defn- effect-phase
  "Execute each effect function. Return an updated flow state."
  [cin dataflow]
  (output-phase cin dataflow :effect))

(defn- derive-fn [derive]
  (let [{input-paths :in derive-fn :fn out-path :out args :args arg-names :arg-names} derive]
    (fn [{:keys [change context] :as state}]
      (if (dataflow/propagate? state input-paths)
        (apply apply-in state out-path derive-fn
               (dataflow/dataflow-fn-args
                (dataflow/flow-input context state input-paths change)
                args
                arg-names))
        state))))

(defn- derive-phase
  "Execute each derive function in dependency order only if some input to the
  function has changed. Return an updated flow state."
  [cin dataflow]
  (run-pipeline cin (map #(channel-fn (derive-fn %)) (:derive dataflow))))

(defn- transform-phase
  "Find the first transform function that matches the message and
  execute it, returning the updated flow state."
  [cin]
  (let [cout (chan)]
    (go (while true
          (let [{:keys [new dataflow context] :as state} (<! cin)
                {out-path :out key :key} ((:input-adapter dataflow) (:message context))
                transform-fn (dataflow/find-message-transformer (:transform dataflow) out-path key)
                new-state (if transform-fn
                            (apply-in state out-path transform-fn (:message context))
                            state)]
            (if (channel? new-state)
              (>! cout (<! new-state))
              (>! cout new-state)))))
    cout))

(defn- flow-phases-step
  "Given a dataflow, a state and a message, run the message through
  the dataflow and return the updated state. The dataflow will be
  run only once."
  [input-channel dataflow]
  (let [cout (chan)
        transform-in (chan)
        transform-out (transform-phase transform-in)
        derive-out (derive-phase transform-out dataflow)
        continue-out (continue-phase derive-out dataflow)]
    (go (while true
          (let [[state message] (<! input-channel)
                state (-> state
                          (update-in [:new] dissoc :continue)
                          (assoc-in [:context :message] message))]
            (>! transform-in state)
            (>! cout [(<! continue-out) message]))))
    cout))

(defn- update-continue-input [result input]
  (if (empty? input)
    result
    (update-in result [:new :continue-inputs] (fnil into []) input)))

(declare run-flow-phases)

(defn- process-continue [state continue dataflow]
  (let [cin (chan)
        cout (chan)
        step-channel (run-flow-phases cin dataflow)]
    (go (loop [messages continue
               state state]
          (if (seq messages)
            (do (>! cin [(assoc state :old (:new state)) (first messages)])
                (recur (rest messages) (<! step-channel)))
            (>! cout state))))
    cout))

(defn- run-flow-phases [input-channel dataflow]
  (let [cout (chan)
        step-channel (flow-phases-step input-channel dataflow)]
    (go (while true
          (let [[state message] (<! step-channel)
                {{continue :continue} :new} state
                input (filter #(:input (meta %)) continue)
                continue (remove #(:input (meta %)) continue)
                new-state (if (empty? continue)
                            (-> state
                                (update-in [:new] dissoc :continue)
                                (update-continue-input input))
                            (process-continue (update-continue-input state input)
                                              continue
                                              dataflow))
                new-state (if (channel? new-state) (<! new-state) new-state)]
            (>! cout new-state))))
    cout))

(defn- run-output-phases [input-channel dataflow]
  (let [cout (chan)
        effect-out (effect-phase input-channel dataflow)
        emit-out (emit-phase effect-out dataflow)]
    (go (while true
          (>! cout (<! emit-out))))
    cout))

(defn- run-all-phases [input-channel dataflow]
  (let [cout (chan)
        flow-phases-input (chan)
        flow-phases-output (run-flow-phases flow-phases-input dataflow)
        output-phases-input (chan)
        output-phases-output (run-output-phases output-phases-input dataflow)]
    (go (while true
          (let [[model message] (<! input-channel)
                state {:old model
                       :new model
                       :change {}
                       :dataflow dataflow
                       :context {}}]
            (>! flow-phases-input [state message])
            (let [new-state (<! flow-phases-output)]
              (>! output-phases-input (assoc-in new-state [:context :message] message))
              (>! cout (:new (<! output-phases-output)))))))
    cout))

(defn build
  "Given a dataflow description map, return a dataflow engine. An example dataflow
  configuration is shown below:

  {:transform [[:op [:output :path] transform-fn]]
   :effect    #{{:fn effect-fn :in #{[:input :path]}}}
   :derive    #{{:fn derive-fn :in #{[:input :path]} :out [:output :path]}}
   :continue  #{{:fn some-continue-fn :in #{[:input :path]}}}
   :emit      [[#{[:input :path]} emit-fn]]}
  "
  [description]
  (let [cin (chan)
        dataflow (-> description
                     (update-in [:transform] dataflow/transform-maps)
                     (update-in [:derive] dataflow/derive-maps)
                     (update-in [:continue] (comp set dataflow/output-maps))
                     (update-in [:effect] (comp set dataflow/output-maps))
                     (update-in [:emit] dataflow/output-maps)
                     (update-in [:derive] dataflow/sort-derive-fns)
                     (update-in [:input-adapter] dataflow/add-default identity))
        flow-output-channel (run-all-phases cin dataflow)]
    (assoc dataflow :in cin :out flow-output-channel)))

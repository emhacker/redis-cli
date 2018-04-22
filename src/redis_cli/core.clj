(ns redis-cli.core
  (:gen-class)
  (:require [clojure.java.io :as io])
  (:require [clojure.tools.logging :as log]))

(def BUF_SIZE 128)

; command names know at compile time.
(def commands {
  :append {:key-begin 1},
  :blpop  {:key-begin 1, :key-stride 1},
  :brpop  {:key-begin 1, :key-stride 1},
  :get    {:key-begin 1},
  :getset {:key-begin 1},
  :keys   {:all-shards true}, 
  :mget   {:key-begin 1, :key-stride 1},
  :mset   {:key-begin 1, :key-stride 2},
  :msetnx {:key-begin 1, :key-stride 2},
  :set    {:key-begin 1},
  :setex  {:key-begin 1},
  :setnx  {:key-begin 1},
  :wait   {:all-shards true}
  })

; state machine is as following:
; |init|             |---      num tokens       --->
; |num tokens|       |---      command name     --->
; |waiting keys|     |--- keys parsing finished --->
; |waiting data|     |---     data finished     --->
; |completed|
(defn create-ctx
  "returns an initial parsing state"
  [] {:state :init,
   :token_parsed 0,
   :num_tokens 0}
)

(defn barray-to-str
  "Converts a given byte-array to a string"
  [barray]
  (new String barray))

(defn _change-state
  "Changes the state in the given context"
  [ctx new-state]
  (assoc ctx :state new-state)
)

(defn init-parse
  [chnk ctx]
  (log/infof "TODO: Init parse pahse")
  (_change-state ctx :num-tokens)
)

(defn num-tokens-parse
  [chnk ctx]
  (log/infof "TODO: # of token parse")
  (_change-state ctx :keys)
)

(defn keys-parse
  [chnk ctx]
  (log/infof "TODO: Parsing keys.")
  (_change-state ctx :data)
)

(defn data-parse
  [chnk ctx]
  (log/infof "TODO: Parsing data.")
  (_change-state ctx :completed)
)

(defn completed-parse
  [chnk ctx]
  (log/infof "TODO: Parsing completed")
  ctx
)

(defn choose-parsef
  "Choose the correct parsing function according the given parsing-state"
 [state]
 (log/infof "choosing function with for state %s\n" state)
 (case state
  :init init-parse
  :num-tokens num-tokens-parse
  :keys keys-parse
  :data data-parse
  :completed completed-parse
  (throw (AssertionError. (str "Unkown state " state))))
)

(defn parse-chunk
  "Parses the given chunk. Side effect is reflected to the return-value (the context)"
  [chnk ctx]
  (log/infof "parse-chunk called with context " ctx)
  (let [parse-f (choose-parsef (get ctx :state))
        ctx (parse-f chnk ctx)]
    (log/infof "Updated context is: " ctx)
    ctx)
)

(defn is-final-state
  "Inidicates whether the given parsing context is in final state."
  [ctx]
  (= (get ctx :state) :completed)
)

(defn parse
  "Parses the the text in the input stream.
  ctx is the context of the parser, which is also the return value for this
  function."
  [^java.io.InputStream is ctx]
  (let [buf (byte-array BUF_SIZE)]
    (loop [total-read 0 lctx ctx]
      (let [bytes-read (.read is buf)
            lctx (parse-chunk (barray-to-str buf) lctx)]
        (if (or (not (is-final-state lctx)) (>= bytes-read BUF_SIZE))
            (recur (+ total-read bytes-read) lctx)
            lctx))))
)

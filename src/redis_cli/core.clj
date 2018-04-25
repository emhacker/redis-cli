(ns redis-cli.core
  (:gen-class)
  (:require [clojure.java.io :as io])
  (:require [clojure.tools.logging :as log])
  (:require [clojure.string :as str]))

(def BUF_SIZE 128)

; command names know at compile time.
(def commands {
  "append" {:key-begin 1},
  "blpop"  {:key-begin 1, :key-stride 1},
  "brpop"  {:key-begin 1, :key-stride 1},
  "get"    {:key-begin 1},
  "getset" {:key-begin 1},
  "keys"   {:all-shards true},
  "mget"   {:key-begin 1, :key-stride 1},
  "mset"   {:key-begin 1, :key-stride 2},
  "msetnx" {:key-begin 1, :key-stride 2},
  "set"    {:key-begin 1},
  "setex"  {:key-begin 1},
  "setnx"  {:key-begin 1},
  "wait"   {:all-shards true}
  })

(defn get-command-symbol
  "Returns the symbol associated with the given command name.
  nil is returned if the command does not reside in the command list"
  [cmd-name]
  (log/infof "Looking for command %s (of length %d)" cmd-name (count cmd-name))
  (if (contains? commands cmd-name)
    (symbol cmd-name)
    nil)
)

; state machine is as following:
; |init|             |---      *       --->
; |num tokens|       |---   Number     --->
; |command name|     |---   String     --->
; |waiting keys|     |---   String     --->
; |waiting data|     |---   String     --->
; |completed|
(defn create-ctx
  "returns an initial parsing state"
  [] {:state :init,
   :command-ty "NO-CMD"
   :buffer-offset 0,
   :token-parsed 0,
   :num-tokens 0}
)

;;
;; Utils
;;
(defn != [l r] (not (= l r)))

(defn tokenize [x] (first (str/split x #"\s+")))

(defn is-digit? [a] (and (>= (int a) (int \0)) (<= (int a) (int \9))))

(defn parse-error
  "Handles a parsing error"
  [msg]
  (str "-ERR" msg)
)

(defn barray-to-str
  "Converts a given byte-array to a string"
  [barray]
  (new String barray)
)

(defn a-to-i
  "Converts the given string to a number (atoi semantics)"
  [s]
  (loop [ret ""
         lvariant (rest s)
         cur (first s)]
    (if (and (some? cur) (is-digit? cur))
      (recur (str ret cur) (rest lvariant) (first lvariant))
      {:value (.  Integer (valueOf (str ret))) :len (count (str ret))}))
)

;;
;; Context setters.
;;
(defn _change-state
  "Changes the state in the given context"
  [ctx new-state & args]
  (if (!= :state :parse-error)
    (assoc ctx :state new-state)
    (assoc (assoc ctx :state :parse-error) :error-msg args))
)

(defn _set-token-number
  "Sets the number of token value for the given context"
  [ctx num-tokens]
  (assoc ctx :num-tokens num-tokens)
)

(defn _set-command-ty
  "Sets the command name for the given context"
  [ctx command-name]
  (let [cmd-ty (get-command-symbol command-name)]
    (if (some? cmd-ty)
      (assoc ctx :command-ty cmd-ty)
      (do
        (log/errorf "Unknown command %s" command-name)
        (_change-state ctx :parse-error))))
)

(defn _incr-buffer-offset
  "Sets the buffer read offset"
  [ctx offset]
  (assoc ctx :buffer-offset (+ offset (get ctx :buffer-offset)))
)

(defn _eat-line-endings
  "'Eats' away line endings from given buffer (reflected in the context).
  If the given buffer doesn't contain line-ending charachters, error is issued."
  [ctx chnk]
  (if (!= (subs chnk 0 2) "\r\n")
    (_incr-buffer-offset ctx 2)
    (do
      (log/errorf "Error eating line ending, the buffer is: %s" chnk)
      (_change-state ctx :parse-error)))
)

(defn _eat-token
  "'Eats' a token (it's length is the last parameter for this function),
   line ending included."
  [ctx chnk token-len]
  (_eat-line-endings
    (_incr-buffer-offset ctx token-len)
    chnk)
)

;;
;; Context accessors.
;;
(defn _get-buffer [ctx chnk] (subs chnk (get ctx :buffer-offset)))

(defn _parse-token
  "Parses the next token from the buffer and returned a structured representation
  of if."
  [ctx, chnk]
  (loop [p-state :$-sign
         l-ctx ctx
         kolboinik nil]
    (case p-state
      :$-sign
      (if (= \$ (first (_get-buffer l-ctx chnk)))
        (recur :token-len (_incr-buffer-offset l-ctx 1) nil)
        (_change-state ctx :parse-error))
      :token-len
      (let [tok-len (a-to-i(_get-buffer l-ctx chnk))]
        (recur
          :token-val
          (_eat-line-endings
            (_incr-buffer-offset l-ctx (get tok-len :len))
            chnk)
          (get tok-len :value)))
      :token-val
        (assoc
          l-ctx
          :last-tok
          (subs
            (_get-buffer l-ctx chnk)
            0
            kolboinik))))
)

;;
;; Parsing state machine function.
;;
(defn init-parse
  "Extract the number of expected tokens, and saves them into the context"
  [ctx chnk]
  (log/debugf "init-parse called with %s %s" chnk ctx)
  (if (!= \* (first chnk))
    (do
      (log/error (parse-error (str "Unkown command " (tokenize chnk))))
      (_change-state ctx :parse-error))
    (_incr-buffer-offset
      (_change-state ctx :num-tokens)
      1))
)

(defn command-name-parse
    "Parses the command name token."
    [ctx chnk]
  (log/infof "command-name-parse Called with ctx %s" ctx)
  (loop [state :dollar-sign
         loop-ctx ctx]
    (case state
      :dollar-sign
      (if (= \$ (first (_get-buffer loop-ctx chnk)))
        (do
          (log/debug "Found $ sign, as expected")
          (recur :token-len (_incr-buffer-offset loop-ctx 1)))
        (do
          (log/infof "Couldn't find a $ sign, the buffer is %s, ctx is: %s"
            (_get-buffer loop-ctx chnk) loop-ctx)
          (_change-state loop-ctx :parse-error)))
      :token-len
      (let [tok-len (a-to-i (_get-buffer loop-ctx chnk))
            len-prefix (int (Math/ceil (Math/log10 tok-len)))
            tmp-ctx (_eat-token loop-ctx chnk len-prefix)]
        (log/infof
          "Parsing command-name: tok-len=%d, len-perfix=%d, tmp-ctx=%s"
          tok-len
          len-prefix
          tmp-ctx)
        (_change-state
          (_eat-token
            (_set-command-ty
              tmp-ctx
              (subs (_get-buffer tmp-ctx chnk) 0 tok-len))
            chnk
            tok-len)
          :keys))))
)

(defn num-tokens-parse
  [ctx chnk]
  (log/infof "Parsing # of tokens with context: %s" ctx)
  (let [num-tokens (a-to-i (_get-buffer ctx chnk))
        char-read (int (Math/ceil (Math/log10 num-tokens)))]
    (log/infof "num-tokens-parse: number of char read is %d" char-read)
    (_change-state
      (_set-token-number
        (_eat-token
          ctx
          chnk
          char-read)
        num-tokens)
      :command-name))
)

(defn keys-parse
  [ctx chnk]
  (log/infof "Parsing key with context: %s" ctx)
  (let [key-token (_parse-token ctx chnk)]
    (_change-state
      (assoc
        ctx :command-keys
        (subs
          (_get-buffer ctx chnk)
          0
          (:value key-token)))
      :data))
)

(defn data-parse
  [ctx chnk]
  (log/infof "TODO: Parsing data.")
  (_change-state ctx :completed)
)

(defn completed-parse
  [ctx chnk]
  (log/infof "TODO: Parsing completed")
  ctx
)

(defn choose-parsef
  "Choose the correct parsing function according the given parsing-state"
 [state]
 (log/infof "choosing function for state %s\n" state)
 (case state
  :init init-parse
  :num-tokens num-tokens-parse
  :command-name command-name-parse
  :keys keys-parse
  :data data-parse
  :completed completed-parse
  (throw (AssertionError. (str "Unkown state " state))))
)

(defn parse-chunk
  "Parses the given chunk. Side effect are reflected to the return-value
  (the context)"
  [chnk ctx]
  (log/info "parse-chunk called with context " ctx)
  (let [parse-f (choose-parsef (get ctx :state))
        ctx (parse-f ctx chnk )]
    (log/infof "Updated context is: %s" ctx)
    ctx)
)

(defn is-final-state?
  "Inidicates whether the given parsing context is in final state."
  [ctx]
  (letfn [(cmp-state [s] (= (get ctx :state) s))]
    (or (cmp-state :completed) (cmp-state :parse-error)))
)

(defn parse
  "Parses the the text in the input stream.
  ctx is the context of the parser, which is also the return value of this
  function."
  [^java.io.InputStream is ctx]
  (let [buf (byte-array BUF_SIZE)]
    (loop [total-read 0 lctx ctx]
      (let [bytes-read (.read is buf)
            lctx (parse-chunk (barray-to-str buf) lctx)]
        (if (or (not (is-final-state? lctx)) (>= bytes-read BUF_SIZE))
            (recur (+ total-read bytes-read) lctx)
            lctx))))
)

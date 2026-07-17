(ns kotobase.gremlin.wire
  "gremlin.kotobase.net -- GraphSON (JSON) RequestMessage/ResponseMessage
  envelope + WebSocket transport for `kotobase.gremlin.traversal`
  (ADR-2607172500 in `com-junkawasaki/root`).

  `.cljs`, NOT `.cljc` -- this namespace requires real socket I/O, so it
  only runs under a Node.js-hosted ClojureScript runtime (nbb in this
  repo), same isolation discipline `kotoba-lang/nostr`'s
  `nostr.relay.transport` and `org-ietf-sftp`'s `kotobase.sftp.transport.*`
  use. It is a pure CONSUMER of `kotobase.gremlin.traversal` (unmodified,
  `.cljc`, zero I/O) -- every actual traversal-translation decision still
  happens there; this namespace only moves bytes, parses/builds GraphSON
  envelopes, and dispatches one WebSocket text frame to one `execute` call.

  ## WebSocket handshake + frame codec -- reused/adapted, not re-derived

  Per ADR-2607172500's explicit instruction, the RFC 6455 handshake
  (SHA-1 accept-key computation via `node:crypto`) and frame codec
  (masking/unmasking, extended length encoding, over raw `node:net`, zero
  `ws` npm dependency) below is adapted directly from `kotoba-lang/nostr`'s
  `nostr.relay.transport` -- that repo already proved this exact transport
  pattern works end-to-end (real cross-connection fan-out test, 2/2
  scenarios; see `nostr.relay.transport`'s own docstring for the full RFC
  6455 scope notes, which apply unchanged here: text frames only, single-
  frame messages only, no permessage-deflate). What differs from
  `nostr.relay.transport` is ONLY the payload semantics carried inside each
  text frame -- Gremlin `RequestMessage`/`ResponseMessage` JSON envelopes
  instead of Nostr's `EVENT`/`REQ`/`OK`/`EOSE` JSON arrays -- and the fact
  that there is no cross-connection fan-out here at all (each request gets
  exactly one response, no subscriptions/push).

  ## GraphSON RequestMessage/ResponseMessage -- an honest v0.1 approximation

  Real TinkerPop GraphSON `RequestMessage`/`ResponseMessage` wraps every
  typed value (including the traversal itself, `g:Bytecode`) in GraphSON's
  typed `{\"@type\": \"...\", \"@value\": ...}` envelope form, and the
  driver-level protocol additionally supports an `\"op\": \"eval\"` mode for
  raw Gremlin script strings. **This repo does NOT implement GraphSON's
  typed `@type`/`@value` wrapper format, and does not support `\"op\":
  \"eval\"` at all (out of scope, ADR-2607172500)** -- being honest about
  this rather than silently claiming full wire fidelity. What IS
  implemented, and real:

  Request (client -> server), one JSON object per WebSocket text frame:
  ```json
  {\"requestId\": \"<any string, echoed back verbatim>\",
   \"op\": \"bytecode\",
   \"processor\": \"traversal\",
   \"args\": {\"gremlin\": [[\"V\"], [\"hasLabel\", \"users\"], [\"values\", \"name\"]]}}
  ```
  `args.gremlin` is a PLAIN JSON array-of-arrays -- this repo's own
  bytecode-shaped EDN (`kotobase.gremlin.traversal`'s ns docstring) encoded
  straight to JSON (keyword step names become JSON strings), not real
  GraphSON-typed Bytecode. `op` MUST be `\"bytecode\"` (`\"eval\"` and any
  other value are rejected with a clear error, never silently misexecuted).

  Response (server -> client), one JSON object per WebSocket text frame:
  ```json
  {\"requestId\": \"<echoed requestId>\",
   \"status\": {\"code\": 200, \"message\": \"\", \"attributes\": {}},
   \"result\": {\"data\": [...], \"meta\": {}}}
  ```
  `status.code` values are modeled on (not a guaranteed-identical
  enumeration of) real TinkerPop Gremlin Server response codes: `200`
  SUCCESS, `597` traversal-translation/execution failure (out-of-scope
  step, malformed bytecode, missing vertex-colls/visible? -- modeled on
  TinkerPop's `597` SCRIPT_EVALUATION_EXCEPTION), `598` malformed request
  (invalid JSON, missing `requestId`/`op`/`args.gremlin`, or `op` !=
  `\"bytecode\"` -- modeled on TinkerPop's `598` MALFORMED_REQUEST). On
  failure `result.data` is `null` and `status.message` carries the error
  text; on success `result.data` is the (sorted, deduplicated) vector
  `kotobase.gremlin.traversal/execute` returned, `result.meta` is always
  `{}` in v0.1 (no `x` pagination / bulk-result metadata)."
  (:require ["node:net" :as net]
            ["node:crypto" :as ncrypto]
            [clojure.string :as str]
            [kotobase.gremlin.json :as json]
            [kotobase.gremlin.traversal :as traversal]))

(def ^:private ws-guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

;; ------------------------------------------------------------- frame codec
;; Adapted from kotoba-lang/nostr's nostr.relay.transport (see ns docstring
;; "WebSocket handshake + frame codec" section) -- unchanged wire-level
;; logic, only the ns/require shape differs.

(defn- unmask!
  "XOR-unmask `payload-buf` (a Buffer) in place against the 4-byte
  `mask-bytes` vector, per RFC 6455 §5.3."
  [payload-buf mask-bytes]
  (dotimes [i (.-length payload-buf)]
    (.writeUInt8 payload-buf
                 (bit-xor (.readUInt8 payload-buf i) (nth mask-bytes (mod i 4)))
                 i))
  payload-buf)

(defn try-decode-frame
  "buf: a Buffer possibly containing 0, 1, or more complete WS frames.
  Returns {:fin? :opcode :payload (Buffer, unmasked) :consumed N} for the
  first complete frame in buf, or nil if buf doesn't yet contain one
  complete frame (caller should wait for more `data`)."
  [buf]
  (when (>= (.-length buf) 2)
    (let [b0 (.readUInt8 buf 0)
          b1 (.readUInt8 buf 1)
          fin? (not (zero? (bit-and b0 0x80)))
          opcode (bit-and b0 0x0f)
          masked? (not (zero? (bit-and b1 0x80)))
          len0 (bit-and b1 0x7f)
          [payload-len header-extra]
          (cond
            (< len0 126) [len0 0]
            (= len0 126) (when (>= (.-length buf) 4) [(.readUInt16BE buf 2) 2])
            :else (when (>= (.-length buf) 10) [(js/Number (.readBigUInt64BE buf 2)) 8]))]
      (when payload-len
        (let [mask-offset (+ 2 header-extra)
              mask-len (if masked? 4 0)
              payload-offset (+ mask-offset mask-len)
              total (+ payload-offset payload-len)]
          (when (>= (.-length buf) total)
            (let [mask-bytes (when masked?
                                [(.readUInt8 buf mask-offset)
                                 (.readUInt8 buf (+ mask-offset 1))
                                 (.readUInt8 buf (+ mask-offset 2))
                                 (.readUInt8 buf (+ mask-offset 3))])
                  payload (.slice buf payload-offset total)]
              (when masked? (unmask! payload mask-bytes))
              {:fin? fin? :opcode opcode :masked? masked? :payload payload :consumed total})))))))

(defn- encode-len-bytes [len]
  (cond
    (< len 126) (js/Buffer.from #js [len])
    (< len 65536) (let [b (js/Buffer.alloc 3)]
                    (.writeUInt8 b 126 0) (.writeUInt16BE b len 1) b)
    :else (let [b (js/Buffer.alloc 9)]
            (.writeUInt8 b 127 0) (.writeBigUInt64BE b (js/BigInt len) 1) b)))

(defn encode-frame
  "Server->client frames are unmasked, per RFC 6455 §5.1."
  [opcode payload-buf]
  (let [b0 (bit-or 0x80 opcode) ;; FIN=1
        header (js/Buffer.concat #js [(js/Buffer.from #js [b0]) (encode-len-bytes (.-length payload-buf))])]
    (js/Buffer.concat #js [header payload-buf])))

(defn encode-text-frame [s] (encode-frame 0x1 (js/Buffer.from s "utf8")))
(defn encode-close-frame ([] (encode-frame 0x8 (js/Buffer.alloc 0)))
  ([payload] (encode-frame 0x8 payload)))
(defn encode-pong-frame [payload] (encode-frame 0xA payload))

;; ---------------------------------------------------------------- handshake

(defn- parse-request-headers
  "Raw ASCII header text (request line + header lines, no body, no trailing
  blank line) -> {lower-case-header-name value}."
  [header-text]
  (into {}
        (keep (fn [line]
                (when-let [idx (str/index-of line ":")]
                  [(str/lower-case (str/trim (subs line 0 idx)))
                   (str/trim (subs line (inc idx)))])))
        (rest (str/split header-text #"\r\n"))))

(defn accept-key
  "RFC 6455 §1.3: base64(sha1(client-key + magic guid))."
  [client-key]
  (-> (.createHash ncrypto "sha1")
      (.update (str client-key ws-guid))
      (.digest "base64")))

;; ---------------------------------------------------------- GraphSON codec

(defn- bytecode-json->edn
  "GraphSON-approximation `args.gremlin` JSON value (a JSON array of JSON
  arrays, e.g. `[[\"V\"] [\"hasLabel\" \"users\"] [\"values\" \"name\"]]`,
  already JSON-parsed to EDN vectors of STRINGS by `kotobase.gremlin.json`)
  -> `kotobase.gremlin.traversal`'s bytecode shape (vectors whose first
  element is a KEYWORD step name, see that ns's docstring)."
  [gremlin]
  (when-not (sequential? gremlin)
    (throw (ex-info "args.gremlin must be a JSON array of [step, ...args] arrays"
                     {:gremlin/code :gremlin/malformed-request})))
  (mapv (fn [tup]
          (when-not (and (sequential? tup) (seq tup) (string? (first tup)))
            (throw (ex-info (str "each args.gremlin entry must be a non-empty array whose"
                                  " first element is a step-name string -- got " (pr-str tup))
                             {:gremlin/code :gremlin/malformed-request})))
          (into [(keyword (first tup))] (rest tup)))
        gremlin))

(defn- request-message->bytecode
  "Parsed RequestMessage JSON (string keys) -> `[request-id bytecode]`, or
  throws (`(:gremlin/code (ex-data e))` = `:gremlin/malformed-request`) for
  anything outside the v0.1 wire subset -- see ns docstring."
  [{:strs [requestId op args]}]
  (when-not (string? requestId)
    (throw (ex-info "RequestMessage missing string \"requestId\"" {:gremlin/code :gremlin/malformed-request})))
  (when (not= "bytecode" op)
    (throw (ex-info (str "unsupported \"op\": " (pr-str op)
                          " -- v0.1 only supports \"op\": \"bytecode\" (no \"eval\", ADR-2607172500)")
                     {:gremlin/code :gremlin/malformed-request})))
  (when-not (map? args)
    (throw (ex-info "RequestMessage missing \"args\" object" {:gremlin/code :gremlin/malformed-request})))
  [requestId (bytecode-json->edn (get args "gremlin"))])

(defn encode-request
  "Build a GraphSON RequestMessage JSON string for `bytecode` (this repo's
  own EDN bytecode shape, see `kotobase.gremlin.traversal` ns docstring).
  `request-id` defaults to a timestamp-based string if omitted (test/demo
  convenience -- real clients should supply their own unique id)."
  ([bytecode] (encode-request bytecode (str "req-" (.now js/Date))))
  ([bytecode request-id]
   (json/encode {"requestId" request-id
                 "op" "bytecode"
                 "processor" "traversal"
                 "args" {"gremlin" (mapv (fn [[step & args]] (into [(name step)] args)) bytecode)}})))

(defn- status-code->message [code]
  (case code 200 "" ""))

(defn encode-response
  "Build a GraphSON ResponseMessage JSON string. `result` -> `{:data [...]}`
  on success (`code` 200); `error` -> `{:message \"...\"}` on failure (`code`
  597 or 598, see ns docstring)."
  [request-id code {:keys [data message]}]
  (json/encode {"requestId" request-id
                "status" {"code" code "message" (or message (status-code->message code)) "attributes" {}}
                "result" {"data" (if (= code 200) data nil) "meta" {}}}))

(defn decode-response
  "Parse a ResponseMessage JSON string (test/client convenience) ->
  `{:request-id :code :message :data}`."
  [s]
  (let [{:strs [requestId status result]} (json/parse s)]
    {:request-id requestId
     :code (get status "code")
     :message (get status "message")
     :data (get result "data")}))

;; --------------------------------------------------------------- dispatch

(defn handle-request-text
  "One incoming WebSocket text-frame payload (a GraphSON RequestMessage
  JSON string) -> a GraphSON ResponseMessage JSON string. Pure w.r.t. `ctx`
  (`{:store :vertex-colls :visible?}`, passed straight to
  `kotobase.gremlin.traversal/execute`) -- no socket concepts in this fn,
  it is unit-testable on its own (see `test/kotobase/gremlin/wire_test.cljs`)
  without opening a real socket."
  [ctx text]
  (let [parsed (try (json/parse text) (catch :default _ ::parse-error))]
    (if (= parsed ::parse-error)
      (encode-response "" 598 {:message "malformed JSON request body"})
      (let [request-id (get parsed "requestId" "")]
        (try
          (let [[request-id bytecode] (request-message->bytecode parsed)
                data (traversal/execute ctx bytecode)]
            (encode-response request-id 200 {:data data}))
          (catch :default e
            (let [ed (ex-data e)
                  code (if (= :gremlin/malformed-request (:gremlin/code ed)) 598 597)
                  msg (or (:gremlin/message ed) (.-message e) (str e))]
              (encode-response request-id code {:message msg}))))))))

;; ------------------------------------------------------------------ server

(defn- send! [socket buf] (.write socket buf))

(defn- process-frames!
  "Drain every complete WS frame currently sitting in @buf-atom, dispatching
  each text frame through `handle-request-text` and writing exactly one
  ResponseMessage text frame back. Leaves any trailing partial frame in
  @buf-atom for the next `data` event."
  [ctx socket buf-atom]
  (loop []
    (when-let [{:keys [opcode payload masked? consumed]} (try-decode-frame @buf-atom)]
      (swap! buf-atom #(.slice % consumed))
      (cond
        ;; RFC 6455 §5.1: server MUST close the connection upon receiving an
        ;; unmasked frame from a client.
        (not masked?)
        (do (send! socket (encode-close-frame))
            (.destroy socket))

        (= opcode 0x1) ;; text
        (do (send! socket (encode-text-frame (handle-request-text ctx (.toString payload "utf8"))))
            (recur))

        (= opcode 0x8) ;; close
        (do (send! socket (encode-close-frame))
            (.end socket))

        (= opcode 0x9) ;; ping
        (do (send! socket (encode-pong-frame payload))
            (recur))

        (= opcode 0xA) ;; pong
        (recur)

        :else (recur)))))

(defn- try-handshake!
  [ctx socket buf-atom handshake-done?-atom]
  (let [buf @buf-atom
        text (.toString buf "latin1")
        idx (str/index-of text "\r\n\r\n")]
    (when idx
      (let [headers (parse-request-headers (subs text 0 idx))
            ws-key (get headers "sec-websocket-key")
            remaining (.slice buf (+ idx 4))]
        (if-not ws-key
          (do (send! socket (js/Buffer.from "HTTP/1.1 400 Bad Request\r\n\r\n" "ascii"))
              (.destroy socket))
          (do
            (send! socket (js/Buffer.from
                           (str "HTTP/1.1 101 Switching Protocols\r\n"
                                "Upgrade: websocket\r\n"
                                "Connection: Upgrade\r\n"
                                "Sec-WebSocket-Accept: " (accept-key ws-key) "\r\n\r\n")
                           "ascii"))
            (reset! handshake-done?-atom true)
            (reset! buf-atom remaining)
            (when (pos? (.-length remaining))
              (process-frames! ctx socket buf-atom))))))))

(defn start-server!
  "opts: `{:port int :store IStore :vertex-colls [coll-key ...] :visible?
  (fn [datom] bool)}` -- the last three form the `ctx` passed straight to
  `kotobase.gremlin.traversal/execute` per request (see that ns's docstring
  for why `:vertex-colls` and `:visible?` are both required, non-defaulted).
  Returns a node-handle map: `{:net-server <node:net Server>}`."
  [{:keys [port store vertex-colls visible?]}]
  (when-not (fn? visible?)
    (throw (ex-info "kotobase.gremlin.wire/start-server! requires :visible? (ADR-2607050500, no permissive default)"
                     {:gremlin/code :gremlin/missing-visible})))
  (let [ctx {:store store :vertex-colls vertex-colls :visible? visible?}
        server (net/createServer
                (fn [socket]
                  (let [buf-atom (atom (js/Buffer.alloc 0))
                        handshake-done?-atom (atom false)]
                    (.on socket "data"
                         (fn [chunk]
                           (swap! buf-atom (fn [b] (js/Buffer.concat #js [b chunk])))
                           (if @handshake-done?-atom
                             (process-frames! ctx socket buf-atom)
                             (try-handshake! ctx socket buf-atom handshake-done?-atom))))
                    (.on socket "error" (fn [_e] nil)))))]
    (.on server "error" (fn [e] (println "kotobase.gremlin.wire: server error" (.-message e))))
    (.listen server port)
    {:net-server server :ctx ctx}))

(defn stop-server!
  "Returns a Promise resolved once the server has actually closed (safe to
  re-start-server! on the same port right after)."
  [{:keys [net-server]}]
  (js/Promise.
   (fn [resolve _reject]
     (if net-server
       (.close net-server (fn [_err] (resolve true)))
       (resolve true)))))

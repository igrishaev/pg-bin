(ns pg-copy.parser
  (:refer-clojure :exclude [val])
  (:require
   [pg-copy.const :as const])
  (:import
   (pg.copy LimitedInputStream)
   (java.math RoundingMode
              BigDecimal)
   (java.time
              LocalDate
              ZoneOffset
              Instant
              LocalTime
              OffsetDateTime
              LocalDateTime
              OffsetTime)
   (java.io InputStream
            DataInputStream)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(defmacro defmethods [multifn dispatch-vals & fn-tail]
  `(do
     ~@(for [dispatch-val dispatch-vals]
         `(defmethod ~multifn ~dispatch-val ~@fn-tail))))

(defmulti -parse-field
  (fn [oid _len _dis]
    oid))

(defmethod -parse-field :default
  [oid len _dis]
  (throw (new RuntimeException
              (format "Don't know how to parse value, type: %s, len: %s"
                      oid len))))

#_:clj-kondo/ignore
(defmethods -parse-field [:raw :bytea :bytes]
  [_ len ^DataInputStream dis]
  (.readNBytes dis len))

(defmethods -parse-field [:skip :_ nil]
  [_oid len ^DataInputStream dis]
  (.skipNBytes dis len)
  const/SKIP)

(defmethod -parse-field :uuid
  [_oid _len ^DataInputStream dis]
  (let [hi (.readLong dis)
        lo (.readLong dis)]
    (new UUID hi lo)))

(defmethods -parse-field [:int2 :short :smallint :smallserial]
  [_oid _len ^DataInputStream dis]
  (.readShort dis))

(defmethods -parse-field [:int4 :int :integer :oid :serial]
  [_oid _len ^DataInputStream dis]
  (.readInt dis))

(defmethods -parse-field [:int8 :bigint :long :bigserial]
  [_oid _len ^DataInputStream dis]
  (.readLong dis))

(defmethod -parse-field :numeric
  [_oid _len ^DataInputStream dis]
  (let [amount (.readShort dis)
        weight (.readShort dis)
        signum (.readShort dis)
        scale  (.readShort dis)
        shorts (short-array amount)]

    (loop [i 0]
      (when-not (= i amount)
        (aset shorts i (.readShort dis))
        (recur (inc i))))

    (if (zero? amount)
      BigDecimal/ZERO
      (let [sb (new StringBuilder)]
        (when-not (zero? signum)
          (.append sb \-))
        (.append sb "0.")
        (loop [i 0]
          (when-not (= i amount)
            (let [digit (aget shorts i)]
              (.append sb (format "%04d" digit)))
            (recur (inc i))))
        (-> (new BigDecimal (.toString sb))
            (.movePointRight (* 4 (+ weight 1)))
            (.setScale scale RoundingMode/DOWN))))))

(defmethods -parse-field [:float4 :float :real]
  [_oid _len ^DataInputStream dis]
  (.readFloat dis))

(defmethods -parse-field [:float8 :double :double-precision]
  [_oid _len ^DataInputStream dis]
  (.readDouble dis))

(defmethods -parse-field [:boolean :bool]
  [_oid _len ^DataInputStream dis]
  (.readBoolean dis))

(defn parse-as-text [len ^DataInputStream dis]
  (let [array (.readNBytes dis len)]
    (new String array const/UTF_8)))

#_:clj-kondo/ignore
(defmethods -parse-field [:text :varchar :enum :name :string]
  [_oid len ^DataInputStream dis]
  (parse-as-text len dis))

(defmethod -parse-field :json
  [_oid len ^DataInputStream dis]
  (parse-as-text len dis))

(defmethod -parse-field :jsonb
  [_oid len ^DataInputStream dis]
  (.skipNBytes dis 1)
  (parse-as-text (dec len) dis))

(defmethod -parse-field :date
  [_oid _len ^DataInputStream dis]
  (let [days (.readInt dis)]
    (LocalDate/ofEpochDay (+ days (.toDays const/PG_DIFF)))))

(defmethods -parse-field [:time :time-without-time-zone]
  [_oid _len ^DataInputStream dis]
  (let [micros (.readLong dis)]
    (LocalTime/ofNanoOfDay (* micros 1000))))

(defmethods -parse-field [:timetz :time-with-time-zone]
  [_oid _len ^DataInputStream dis]
  (let [micros (.readLong dis)
        offset (.readInt dis)]
    (OffsetTime/of (LocalTime/ofNanoOfDay (* micros 1000))
                   (ZoneOffset/ofTotalSeconds (- offset)))))

(defmethods -parse-field [:timestamp :timestamp-without-time-zone]
  [_oid _len ^DataInputStream dis]
  (let [payload
        (.readLong dis)

        seconds
        (-> payload
            (/ 1000000)
            (+ (.toSeconds const/PG_DIFF)))

        nanos
        (-> payload
            (mod 1000000)
            (* 1000))]

    (LocalDateTime/ofEpochSecond seconds (int nanos) ZoneOffset/UTC)))

(defmethods -parse-field [:timestamptz :timestamp-with-time-zone]
  [_oid _len ^DataInputStream dis]
  (let [payload
        (.readLong dis)

        seconds
        (-> payload
            (/ 1000000)
            (+ (.toSeconds const/PG_DIFF)))

        nanos
        (-> payload
            (mod 1000000)
            (* 1000))

        inst
        (Instant/ofEpochSecond seconds nanos)]

    (OffsetDateTime/ofInstant inst ZoneOffset/UTC)))

(defn parse-line [^DataInputStream dis columns]
  (let [n (.readShort dis)]
    (when (> n -1)
      (loop [i 0
             off 2
             result []]
        (if (= i n)
          (with-meta result {:pg/length off})
          (let [len (.readInt dis)
                off (+ off 4)
                oid (get columns i)
                val (when-not (= len -1)
                      (-parse-field (or oid :skip) len dis))
                off (if (= len -1)
                      off
                      (+ off len))]
            (cond
              (nil? oid)
              (recur (inc i) off result)
              (= val const/SKIP)
              (recur (inc i) off result)
              :else
              (recur (inc i) off (conj result val)))))))))

(defn parse
  [^InputStream in columns]
  (let [columns (vec columns)

        -step
        (fn -step [^DataInputStream -dis i off]
          (lazy-seq
           (when-let [line (parse-line -dis columns)]
             (let [len (-> line meta :pg/length)]
               (cons (vary-meta line
                                assoc
                                :pg/index i
                                :pg/offset off)
                     (-step -dis (inc i) (+ off len)))))))

        dis
        (new DataInputStream in)

        skip (count const/COPY_HEADER)]

    (.skipNBytes dis skip)
    (-step dis 0 skip)))

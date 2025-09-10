(ns pg-copy.parser
  (:require
   [pg-copy.const :as const]
   [clojure.java.io :as io])
  (:import
   (org.pg.copy OpenInputStream)
   (java.math RoundingMode
              BigDecimal)
   (java.time Duration
              LocalDate
              ZoneOffset
              Instant
              LocalTime
              OffsetDateTime
              LocalDateTime
              OffsetTime)
   (java.io InputStream
            DataInputStream
            DataOutputStream)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(defmacro defmethods [multifn dispatch-vals & fn-tail]
  `(do
     ~@(for [dispatch-val dispatch-vals]
         `(defmethod ~multifn ~dispatch-val ~@fn-tail))))

(defmulti -parse-field
  (fn [oid _len _dis _opt]
    oid))

(defmethod -parse-field :default
  [oid len ^DataInputStream dis _opt]
  (throw (new RuntimeException
              (format "Don't know how to parse value, type: %s, len: %s"
                      oid len))))

(defmethods -parse-field [:raw :bytea]
  [_ len ^DataInputStream dis _opt]
  (.readNBytes dis len))

(defmethod -parse-field :skip
  [_oid len ^DataInputStream dis _opt]
  (.skipNBytes dis len)
  ::skip)

(defmethod -parse-field :uuid
  [_oid _len ^DataInputStream dis _opt]
  (let [hi (.readLong dis)
        lo (.readLong dis)]
    (new UUID hi lo)))

(defmethod -parse-field :int2
  [_oid _len ^DataInputStream dis _opt]
  (.readShort dis))

(defmethod -parse-field :int4
  [_oid _len ^DataInputStream dis _opt]
  (.readInt dis))

(defmethod -parse-field :int8
  [_oid _len ^DataInputStream dis _opt]
  (.readLong dis))

(defmethod -parse-field :numeric
  [_oid _len ^DataInputStream dis _opt]
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

(defmethod -parse-field :float4
  [_oid _len ^DataInputStream dis _opt]
  (.readFloat dis))

(defmethod -parse-field :float8
  [_oid _len ^DataInputStream dis _opt]
  (.readDouble dis))

(defmethod -parse-field :boolean
  [_oid _len ^DataInputStream dis _opt]
  (.readBoolean dis))

(defn parse-as-text [len ^DataInputStream dis _opt]
  (let [array (.readNBytes dis len)]
    (new String array const/UTF_8)))

(defmethods -parse-field [:text :varchar]
  [_oid len ^DataInputStream dis _opt]
  (parse-as-text len dis))

(defmethod -parse-field :json
  [_oid len ^DataInputStream dis {:keys [fn-json-decode]}]
  (cond-> (parse-as-text len dis)
    fn-json-decode
    fn-json-decode))

(defmethod -parse-field :jsonb
  [_oid len ^DataInputStream dis {:keys [fn-json-decode]}]
  (.skipNBytes dis 1)
  (cond-> (parse-as-text (dec len) dis)
    fn-json-decode
    fn-json-decode))

(defmethod -parse-field :date
  [_oid len ^DataInputStream dis _opt]
  (let [days (.readInt dis)]
    (LocalDate/ofEpochDay (+ days (.toDays const/PG_DIFF)))))

(defmethod -parse-field :time
  [_oid len ^DataInputStream dis _opt]
  (let [micros (.readLong dis)]
    (LocalTime/ofNanoOfDay (* micros 1000))))

(defmethod -parse-field :time
  [_oid len ^DataInputStream dis _opt]
  (let [micros (.readLong dis)
        offset (.readInt dis)]
    (OffsetTime/of (LocalTime/ofNanoOfDay (* micros 1000))
                   (ZoneOffset/ofTotalSeconds (- offset)))))

(defmethod -parse-field :timestamp
  [_oid len ^DataInputStream dis _opt]
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

(defmethod -parse-field :timestamptz
  [_oid len ^DataInputStream dis _opt]
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


(defn parse-line [^DataInputStream dis n columns opt]
  (loop [i 0
         pos 2
         result []]
    (if (= i n)
      [pos result]
      (let [len (.readInt dis)
            pos (+ pos 4)
            oid (nth columns i)]
        (if (= len -1)
          (recur (inc i) pos (conj result nil))
          (let [value (-parse-field oid len dis opt)]
            (if (= value ::skip)
              (recur (inc i) (+ pos len) result)
              (recur (inc i) (+ pos len) (conj result value)))))))))


(defn parse

  ([^InputStream in columns]
   (parse in columns nil))

  ([^InputStream in columns opt]
   (let [columns (vec columns)

         -step
         (fn -step [^DataInputStream -dis i off]
           (lazy-seq
            (let [n (.readShort -dis)]
              (when-not (= n -1)
                (let [[len line] (parse-line -dis n columns opt)]
                  (cons (vary-meta line
                                   assoc
                                   :pg/index i
                                   :pg/offset off
                                   :pg/length len)
                        (-step -dis (inc i) (+ off len))))))))

         dis
         (new DataInputStream in)]

     (let [skip (count const/COPY_HEADER)]
       (.skipNBytes dis skip)
       (-step dis 0 skip)))))

(ns pg-copy.core
  (:require
   [clojure.java.io :as io])
  (:import
   (java.time Duration
              LocalDate
              ZoneOffset
              Instant
              LocalTime
              OffsetDateTime
              LocalDateTime
              OffsetTime)
   (java.nio.charset StandardCharsets)
   (java.io InputStream
            DataInputStream
            DataOutputStream)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(defmacro defmethods [multifn dispatch-vals & fn-tail]
  `(do
     ~@(for [dispatch-val dispatch-vals]
         `(defmethod ~multifn ~dispatch-val ~@fn-tail))))


(def ^Duration PG_DIFF
  (Duration/between Instant/EPOCH
                    (-> (LocalDate/of 2000 1 1)
                        (.atStartOfDay(ZoneOffset/UTC)))))

(def ^bytes COPY_HEADER
  (byte-array [(byte \P)
               (byte \G)
               (byte \C)
               (byte \O)
               (byte \P)
               (byte \Y)

               10
               0xFF
               13
               10
               0

               0 0 0 0
               0 0 0 0
               ;;
               ]))

(def ^bytes COPY_TERM
  (byte-array [-1 -1]))


(defmulti -parse-field
  (fn [oid _len _dis]
    oid))

(defmethod -parse-field :default
  [_ len ^DataInputStream dis]
  (.readNBytes dis len))

(defmethod -parse-field "uuid"
  [_oid _len ^DataInputStream dis]
  (let [hi (.readLong dis)
        lo (.readLong dis)]
    (new UUID hi lo)))

(defmethod -parse-field "int2"
  [_oid _len ^DataInputStream dis]
  (.readShort dis))

(defmethod -parse-field "int4"
  [_oid _len ^DataInputStream dis]
  (.readInt dis))

(defmethod -parse-field "int8"
  [_oid _len ^DataInputStream dis]
  (.readLong dis))

;; TODO numeric

(defmethod -parse-field "float4"
  [_oid _len ^DataInputStream dis]
  (.readFloat dis))

(defmethod -parse-field "float8"
  [_oid _len ^DataInputStream dis]
  (.readDouble dis))

(defmethod -parse-field "boolean"
  [_oid _len ^DataInputStream dis]
  (.readBoolean dis))

(defn parse-as-text [len ^DataInputStream dis]
  (let [array (.readNBytes dis len)]
    (new String array StandardCharsets/UTF_8)))

(defmethod -parse-field "text"
  [_oid len ^DataInputStream dis]
  (parse-as-text len dis))

(defmethod -parse-field "varchar"
  [_oid len ^DataInputStream dis]
  (parse-as-text len dis))

(defmethod -parse-field "json"
  [_oid len ^DataInputStream dis]
  (parse-as-text len dis))

(defmethod -parse-field "jsonb"
  [_oid len ^DataInputStream dis]
  (.skipNBytes dis 1)
  (parse-as-text (dec len) dis))

#_
(defmethods -parse-field ["text" "varchar" "json"]
  [_oid len ^DataInputStream dis]
  (let [array (.readNBytes dis len)]
    (new String array StandardCharsets/UTF_8)))

#_
(defmethod -parse-field "jsonb"
  [_oid len ^DataInputStream dis]
  (.skipNBytes dis 1)
  (let [array (.readNBytes dis (dec len))]
    (new String array StandardCharsets/UTF_8)))

(defmethod -parse-field "jsonb"
  [_oid len ^DataInputStream dis]
  (.skipNBytes dis 1)
  (let [array (.readNBytes dis (dec len))]
    (new String array StandardCharsets/UTF_8)))

(defmethod -parse-field "date"
  [_oid len ^DataInputStream dis]
  (let [days (.readInt dis)]
    (LocalDate/ofEpochDay (+ days (.toDays PG_DIFF)))))

(defmethod -parse-field "time"
  [_oid len ^DataInputStream dis]
  (let [micros (.readLong dis)]
    (LocalTime/ofNanoOfDay (* micros 1000))))

(defmethod -parse-field "time"
  [_oid len ^DataInputStream dis]
  (let [micros (.readLong dis)
        offset (.readInt dis)]
    (OffsetTime/of (LocalTime/ofNanoOfDay (* micros 1000))
                   (ZoneOffset/ofTotalSeconds (- offset)))))

(defmethod -parse-field "timestamp"
  [_oid len ^DataInputStream dis]
  (let [payload
        (.readLong dis)

        seconds
        (-> payload
            (/ 1000000)
            (+ (.toSeconds PG_DIFF)))

        nanos
        (-> payload
            (mod 1000000)
            (* 1000))]

    (LocalDateTime/ofEpochSecond seconds (int nanos) ZoneOffset/UTC)))

(defmethod -parse-field "timestamptz"
  [_oid len ^DataInputStream dis]
  (let [payload
        (.readLong dis)

        seconds
        (-> payload
            (/ 1000000)
            (+ (.toSeconds PG_DIFF)))

        nanos
        (-> payload
            (mod 1000000)
            (* 1000))

        inst
        (Instant/ofEpochSecond seconds nanos)]

    (OffsetDateTime/ofInstant inst ZoneOffset/UTC)))


(defn parse-line [^DataInputStream dis n types]
  (loop [i 0
         pos 2
         result []]
    (if (= i n)
      [pos result]
      (let [len (.readInt dis)
            pos (+ pos 4)
            oid (nth types i)]
        (if (= len -1)
          (recur (inc i) pos (conj result nil))
          (let [value (-parse-field oid len dis)]
            (recur (inc i) (+ pos len) (conj result value)))))))

  #_
  (for [i (range n)]
    (let [len (.readInt dis)]
      (when-not (= len -1)
        (let [type (nth types i)]
          (-parse-field type len dis))))))


(defn parse-lines [^InputStream in types]

  (let [types (vec types)

        -step
        (fn -step [^DataInputStream -dis i off]
          (lazy-seq
           (let [n (.readShort -dis)]
             (when-not (= n -1)
               (let [[len line] (parse-line -dis n types)]
                 (cons (vary-meta line assoc ::index i ::offset off ::length len)
                       (-step -dis (inc i) (+ off len))))))))

        dis
        (new DataInputStream in)]

    (let [skip (count COPY_HEADER)]
      (.skipNBytes dis skip)
      (-step dis 0 skip))))



(defmulti -write-field
  (fn [oid val _dos]
    [(type val) oid]))


(defmethod -write-field [UUID "uuid"]
  [_oid ^UUID val ^DataOutputStream dos]
  (let [hi (.getMostSignificantBits val)
        lo (.getLeastSignificantBits val)]
    (doto dos
      (.writeInt 16)
      (.writeLong hi)
      (.writeLong lo))))



(defmethod -write-field [UUID "text"]
  [_oid ^UUID val ^DataOutputStream dos]
  (let [arr (-> val .toString (.getBytes StandardCharsets/UTF_8))]
    (doto dos
      (.writeInt (alength arr))
      (.write arr))))


#_
(comment

  (deftype Foo [^DataInputStream -dis
                ^:unsynchronized-mutable -n]

    java.util.Iterator

    (hasNext [this]
      (when-not (nil? -n)
        (set! -n (.readShort -dis)))
      (not= -n -1))

    (next [this]
      (read-line -dis -n)
      (set! -n nil)))


  (def -lines
    (with-open [in (io/input-stream "...")]
      (doall (parse-lines in ["uuid" "uuid" "jsonb"]))))

  )

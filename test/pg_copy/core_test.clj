(ns pg-copy.core-test
  (:import
   (clojure.lang LazySeq))
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [pg-copy.core :as copy]
   taggie.core))

(def FIELDS
  [:int2
   :int4
   :int8
   :boolean
   :float4
   :float8
   :text
   :varchar
   :time
   :timetz
   :date
   :timestamp
   :timestamptz
   :bytea
   :json
   :jsonb
   :uuid
   :numeric
   :text])

(def DUMP_PATH
  "test/resources/dump.bin")

(defn =bytes [ba]
  (reify Object
    (equals [_ other]
      (and (bytes? other)
           (= (vec ba) (vec other))))))

(deftest test-parse-ok
  (testing "it works!"
    (let [result
          (copy/parse DUMP_PATH FIELDS)]

      (is (vector? result))

      (is (= [[1
               2
               3
               true
               (float 123.456)
               654.321
               "hello"
               "world"
               #LocalTime "10:42:35"
               #OffsetTime "10:42:35+00:30"
               #LocalDate "2025-11-30"
               #LocalDateTime "2025-11-30T10:42:35"
               #OffsetDateTime "2025-11-30T10:12:35.123567Z"
               (=bytes [-34, -83, -66, -17])
               "{\"foo\": [1, 2, 3, {\"kek\": [true, false, null]}]}"
               "{\"foo\": [1, 2, 3, {\"kek\": [true, false, null]}]}"
               #uuid "4bda6037-1c37-4051-9898-13b82f1bd712"
               123456.123M
               nil]]
             result))

      ))

  (testing "it's lazy"
    (with-open [in (io/input-stream DUMP_PATH)]
      (let [result (copy/parse-seq in FIELDS)]
        (is (instance? LazySeq result)))))

  ;; todo: keep only existing fieds
  (testing "leading fields only"
    (let [result
          (copy/parse DUMP_PATH (take 3 FIELDS))]
      (is (= [1
              2
              3
              (=bytes [1])
              (=bytes [66, -10, -23, 121])]
             (->> result
                  first
                  (take 5))))))

  (testing "skip and underscore"
    (let [result
          (copy/parse DUMP_PATH [:int2
                                 :skip
                                 :_
                                 :boolean])]
      (is (= 1
             result))))

  ;; check columns count
  ;; underscore skip
  ;; raw
  ;; unknown type
  ;; keyword strings symbols
  ;; try hyphens
  ;; time-with-time-zone
  ;; timestamp-with-time-zone
  ;; enum
  ;; json cheshire jsonista charred data.json jsam
  ;; rename project
  ;; tidy project.clj
  ;; readme
  ;; release

  )

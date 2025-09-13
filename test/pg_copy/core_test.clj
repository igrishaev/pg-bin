(ns pg-copy.core-test
  (:import
   (clojure.lang LazySeq))
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [jsonista.core :as jsonista]
   [pg-copy.core :as copy]
   [pg-copy.json :as json]
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
   :text
   :decimal])

(def DUMP_PATH
  "test/resources/dump.bin")

(defn =bytes [ba]
  (reify Object
    (equals [_ other]
      (and (bytes? other)
           (= (vec ba) (vec other))))))

(deftest test-parse-ok
  (json/set-string)
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
               nil
               123999.999100500M]]
             result))))

  (testing "it's lazy"
    (with-open [in (io/input-stream DUMP_PATH)]
      (let [result (copy/parse-seq in FIELDS)]
        (is (instance? LazySeq result)))))

  (testing "leading fields only"
    (let [result
          (copy/parse DUMP_PATH (take 3 FIELDS))]
      (is (= [1 2 3]
             (->> result
                  first
                  (take 5))))))

  (testing "one field"
    (let [result
          (copy/parse DUMP_PATH [:int2])]
      (is (= [1]
             (first result)))))

  (testing "empty fields"
    (let [result
          (copy/parse DUMP_PATH [])]
      (is (= []
             (first result)))))

  (testing "skip and underscore"
    (let [result
          (copy/parse DUMP_PATH [:int2
                                 :skip
                                 :_
                                 :boolean])]
      (is (= [1 true]
             (first result)))))

  (testing "raw"
    (let [result
          (copy/parse DUMP_PATH [:int2
                                 :raw
                                 :raw
                                 :boolean])]
      (is (= [1
              (=bytes [0, 0, 0, 2])
              (=bytes [0, 0, 0, 0, 0, 0, 0, 3])
              true]
             (first result)))))

  (testing "lines meta"
    (let [result (copy/parse DUMP_PATH FIELDS)]
      (is (= #:pg{:length 306, :index 0, :offset 19}
             (-> result
                 first
                 meta))))
    (let [result (copy/parse DUMP_PATH [])]
      (is (= #:pg{:length 306, :index 0, :offset 19}
             (-> result
                 first
                 meta)))))

  (testing "unknown type"
    (try
      (copy/parse DUMP_PATH [:foo])
      (is false)
      (catch RuntimeException e
        (is (= "Don't know how to parse value, type: :foo, len: 2"
               (ex-message e))))))

  (testing "nil column"
    (let [result
          (copy/parse DUMP_PATH [:int2
                                 nil
                                 nil
                                 :boolean])]
      (is (= [1 true]
             (first result)))))

  (testing "custom aliases"
    (let [result
          (copy/parse DUMP_PATH [:short
                                 :int
                                 :long
                                 :bool
                                 :float
                                 :double
                                 :string
                                 :enum
                                 :time-without-time-zone
                                 :time-with-time-zone
                                 :date
                                 :timestamp-without-time-zone
                                 :timestamp-with-time-zone
                                 :bytes
                                 :json
                                 :jsonb
                                 :uuid
                                 :decimal
                                 :name
                                 :numeric])]
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
               nil
               123999.999100500M]]
             result)))))

(deftest test-json-cheshire
  (json/set-cheshire keyword)
  (let [result
        (copy/parse DUMP_PATH FIELDS)]
    (is (= [{:foo [1 2 3 {:kek [true false nil]}]}
            {:foo [1 2 3 {:kek [true false nil]}]}]
           (-> result
               first
               (subvec 14 16))))))

(deftest test-json-data
  (json/set-data-json {:key-fn str/upper-case})
  (let [result
        (copy/parse DUMP_PATH FIELDS)]
    (is (= [{"FOO" [1 2 3 {"KEK" [true false nil]}]}
            {"FOO" [1 2 3 {"KEK" [true false nil]}]}]
           (-> result
               first
               (subvec 14 16))))))

(deftest test-json-jsonista
  (json/set-jsonista jsonista/keyword-keys-object-mapper)
  (let [result
        (copy/parse DUMP_PATH FIELDS)]
    (is (= [{:foo [1 2 3 {:kek [true false nil]}]}
            {:foo [1 2 3 {:kek [true false nil]}]}]
           (-> result
               first
               (subvec 14 16))))))

(deftest test-charred
  (json/set-charred {:key-fn str/upper-case})
  (let [result
        (copy/parse DUMP_PATH FIELDS)]
    (is (= [{"FOO" [1 2 3 {"KEK" [true false nil]}]}
            {"FOO" [1 2 3 {"KEK" [true false nil]}]}]
           (-> result
               first
               (subvec 14 16))))))

(deftest test-jsam
  (json/set-jsam {:fn-key str/upper-case})
  (let [result
        (copy/parse DUMP_PATH FIELDS)]
    (is (= [{"FOO" [1 2 3 {"KEK" [true false nil]}]}
            {"FOO" [1 2 3 {"KEK" [true false nil]}]}]
           (-> result
               first
               (subvec 14 16))))))

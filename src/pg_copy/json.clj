(ns pg-copy.json
  (:import
   (java.io DataInputStream)
   (pg.copy LimitedInputStream))
  (:require
   [pg-copy.const :as const]
   [pg-copy.parser :refer [-parse-field]]))

(set! *warn-on-reflection* true)

(defmacro throw!
  ([message]
   `(throw (new RuntimeException ~message)))
  ([template & args]
   `(throw (new RuntimeException (format ~template ~@args)))))

(defn require! [fq-symbol]
  (-> fq-symbol
      requiring-resolve
      (or (throw! "cannot resolve a symbol: %s" fq-symbol))
      (deref)))

(defmacro set-string
  "
  Parse json and jsonb types as plain strings (default
  implementation).
  "
  []
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag `Integer})
        dis (with-meta (gensym "dis") {:tag `DataInputStream})]
    `(do
       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (let [array# (.readNBytes ~dis ~len)]
           (new String array# const/UTF_8)))

       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (let [array# (.readNBytes ~dis (dec ~len))]
           (new String array# const/UTF_8))))))


(defmacro set-cheshire
  "
  Parse json and jsonb types using Cheshire assuming you
  have it in classpath. Accepts an optional function to
  transform keys (see cheshire docs).
  "
  [& [key-fn]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag 'java.lang.Integer})
        dis (with-meta (gensym "dis") {:tag 'java.io.DataInputStream})
        lis (with-meta (gensym "lis") {:tag 'java.io.InputStream})]
    `(let [func# (require! 'cheshire.core/parse-stream)]
       (defmethod -parse-field :json
         [~oid ~len ~dis]
         ;;
         ;; Here and below: wrap the origin stream with LimitedInputStream
         ;; because some JSON parsers read more bytes than they actually
         ;; consume. Such greediness will ruin the entire pipeline.
         ;;
         (with-open [~lis (new LimitedInputStream ~dis ~len)
                     rdr# (io/reader ~lis)]
           (func# rdr# ~@(when key-fn [key-fn]))))
       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [~lis (new LimitedInputStream ~dis (dec ~len))
                     rdr# (io/reader ~lis)]
           (func# rdr# ~@(when key-fn [key-fn])))))))


(defmacro set-data-json
  "
  Parse json and jsonb types using clojure.data.json
  library assuming you have it in classpath. Accepts
  an optional map of reading parameters (see the docs).
  "
  [& [opt-read]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag `Integer})
        dis (with-meta (gensym "dis") {:tag `DataInputStream})]
    `(let [func# (require! 'clojure.data.json/read)]
       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (with-open [lis# (new LimitedInputStream ~dis ~len)
                     rdr# (io/reader lis#)]
           (func# rdr# ~@(when opt-read (mapcat identity opt-read)))))
       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [lis# (new LimitedInputStream ~dis (dec ~len))
                     rdr# (io/reader lis#)]
           (func# rdr# ~@(when opt-read (mapcat identity opt-read))))))))

(defmacro set-jsonista
  "
  Parse json and jsonb types using Jsonista library
  assuming you have it in classpath. Accepts an optional
  ObjectMapper instance for custom data processing
  (see the docs).
  "
  [& [obj-mapper]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag `Integer})
        dis (with-meta (gensym "dis") {:tag `DataInputStream})]
    `(let [func# (require! 'jsonista.core/read-value)]
       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (with-open [lis# (new LimitedInputStream ~dis ~len)]
           (func# lis# ~@(when obj-mapper [obj-mapper]))))
       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [lis# (new LimitedInputStream ~dis (dec ~len))]
           (func# lis# ~@(when obj-mapper [obj-mapper])))))))


(defmacro set-charred
  "
  Parse json and jsonb types using Charred library
  assuming you have it in classpath. Accepts an optional
  map of reading parameters (see the docs).
  "
  [& [opt-read]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag `Integer})
        dis (with-meta (gensym "dis") {:tag `DataInputStream})]
    `(let [func# (require! 'charred.api/read-json)]
       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (with-open [lis# (new LimitedInputStream ~dis ~len)
                     rdr# (io/reader lis#)]
           (func# rdr# ~@(when opt-read (mapcat identity opt-read)))))
       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [lis# (new LimitedInputStream ~dis (dec ~len))
                     rdr# (io/reader lis#)]
           (func# rdr# ~@(when opt-read (mapcat identity opt-read))))))))


(defmacro set-jsam
  "
  Parse json and jsonb types using Jsam library assuming
  you have it in classpath. Accepts an optional map of
  reading parameters (see cheshire docs).
  "
  [& [opt-read]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag 'java.lang.Integer})
        dis (with-meta (gensym "dis") {:tag 'java.io.DataInputStream})
        lis (with-meta (gensym "lis") {:tag 'java.io.InputStream})]
    `(let [func# (require! 'jsam.core/read)]
       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (with-open [~lis (new LimitedInputStream ~dis ~len)]
           (func# ~lis ~@(when opt-read [opt-read]))))
       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [~lis (new LimitedInputStream ~dis (dec ~len))]
           (func# ~lis ~@(when opt-read [opt-read])))))))

(ns pg-copy.json
  (:import
   (java.io DataInputStream)
   (pg.copy LimitedInputStream))
  (:require
   [pg-copy.parser :refer [-parse-field]]))

(defmacro set-cheshire [& [opt-read]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag `Integer})
        dis (with-meta (gensym "dis") {:tag `DataInputStream})
        lis (with-meta (gensym "lis") {:tag `InputStream})]
    `(do
       (require 'cheshire.core)

       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (with-open [~lis (new LimitedInputStream ~dis ~len)]
           (cheshire.core/parse-stream ~lis ~@(when opt-read [opt-read]))))

       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [~lis (new LimitedInputStream ~dis (dec ~len))]
           (cheshire.core/parse-stream ~lis ~@(when opt-read [opt-read])))))))


(defmacro set-jsonista [& [obj-mapper]])

(defmacro set-charred [& [opt-read]])

(defmacro set-jsam [& [opt-read]])

(defmacro set-data-json [& [opt-read]]
  (let [oid (gensym "oid")
        len (with-meta (gensym "len") {:tag `Integer})
        dis (with-meta (gensym "dis") {:tag `DataInputStream})]
    `(do
       (require 'clojure.data.json)

       (defmethod -parse-field :json
         [~oid ~len ~dis]
         (with-open [lis# (new LimitedInputStream ~dis ~len)
                     r# (io/reader lis#)]
           (clojure.data.json/read r# ~@(when opt-read [opt-read]))))

       (defmethod -parse-field :jsonb
         [~oid ~len ~dis]
         (.skipNBytes ~dis 1)
         (with-open [lis# (new LimitedInputStream ~dis (dec ~len))
                     r# (io/reader lis#)]
           (clojure.data.json/read r# ~@(when opt-read [opt-read])))))))



#_
(set-cheshire foo bar)

(ns pg-copy.core
  (:require
   [clojure.java.io :as io]
   [pg-copy.parser :as parser]))

(set! *warn-on-reflection* true)


;; TODO: names

(defn parse-seq [in columns]
  (parser/parse in columns))

(defn parse [src columns]
  (with-open [in (io/input-stream src)]
    (vec (parser/parse in columns))))


#_
(parse "/Users/ivan/Downloads/dump.bin"
       [:int2 :int4 :int8 :boolean
        :float4 :float8 :text :varchar
        :time :timetz :date :timestamp
        :timestamptz :bytea :json :jsonb :uuid])

(ns pg-copy.core
  "
  General namespace with user-friendly API.
  "
  (:require
   [clojure.java.io :as io]
   [pg-copy.json :as json]
   [pg-copy.parser :as parser]))

(set! *warn-on-reflection* true)

(json/set-string)

(defn parse-seq [in columns]
  (parser/parse in columns))

(defn parse [src columns]
  (with-open [in (io/input-stream src)]
    (vec (parser/parse in columns))))

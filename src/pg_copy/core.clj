(ns pg-copy.core
  "
  General namespace with user-friendly API.
  "
  (:require
   [clojure.java.io :as io]
   [pg-copy.parser :as parser]))

(set! *warn-on-reflection* true)

;; TODO names
;; TODO docstrings

(defn parse-seq [in columns]
  (parser/parse in columns))

(defn parse [src columns]
  (with-open [in (io/input-stream src)]
    (vec (parser/parse in columns))))

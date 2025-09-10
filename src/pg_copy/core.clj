(ns pg-copy.core
  (:require
   [pg-copy.parser :as parser]))

(set! *warn-on-reflection* true)


(defn parse
  ([in columns]
   (parser/parse in columns))

  ([in columns opt]
   (parser/parse in columns opt)))

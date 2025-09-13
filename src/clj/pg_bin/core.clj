(ns pg-bin.core
  "
  General namespace with user-friendly API.
  "
  (:require
   [clojure.java.io :as io]
   [pg-bin.json :as json]
   [pg-bin.parser :as parser]))

(set! *warn-on-reflection* true)

(json/set-string)

(defn parse
  "
  Parse a source which might be a file, an input stream,
  a path to a file, etc (enything that io/input-stream
  accepts). The columns is a vector of keywords with
  type names like :integer, :text, and so on (see readme).
  Return a vector of parsed rows.
  "
  [src columns]
  (with-open [in (io/input-stream src)]
    (vec (parser/parse in columns))))

(defn parse-seq
  "
  Lazily parse an input stream with a binary COPY payload.
  Return a lazy sequence of vectors. Must be called within
  the `with-open` macro. The columns is a vector of keywords
  with type names like :integer, :text, and other (see readme).
  "
  [input-stream columns]
  (parser/parse input-stream columns))

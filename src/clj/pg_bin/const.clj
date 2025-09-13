(ns pg-bin.const
  "
  Constant values.
  "
  (:import
   (clojure.lang Keyword)
   (java.nio.charset Charset
                     StandardCharsets)
   (java.time Duration
              Instant
              LocalDate
              ZoneOffset)))

(set! *warn-on-reflection* true)

(def ^Keyword ^:const SKIP
  ::__skip__)

(def ^Duration PG_DIFF
  "
  Difference between Unix epoch and Postgres epoch.
  "
  (Duration/between Instant/EPOCH
                    (-> (LocalDate/of 2000 1 1)
                        (.atStartOfDay ZoneOffset/UTC))))


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

               0 0 0 0 ;; dummy int, flags
               0 0 0 0 ;; dummy int, flags
               ]))

(def ^bytes COPY_TERM
  (byte-array [-1 -1]))

(def ^Charset UTF_8
  StandardCharsets/UTF_8)

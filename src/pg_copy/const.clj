(ns pg-copy.const
  (:import
   (java.time Duration
              Instant
              LocalDate
              ZoneOffset)
   (java.nio.charset Charset
                     StandardCharsets)))

(set! *warn-on-reflection* true)

(def ^Duration PG_DIFF
  (Duration/between Instant/EPOCH
                    (-> (LocalDate/of 2000 1 1)
                        (.atStartOfDay(ZoneOffset/UTC)))))


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

               0 0 0 0
               0 0 0 0]))

(def ^bytes COPY_TERM
  (byte-array [-1 -1]))

(def ^Charset UTF_8
  StandardCharsets/UTF_8)

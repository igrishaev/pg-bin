(defmulti -write-field
  (fn [oid val _dos]
    [(type val) oid]))


(defmethod -write-field [UUID "uuid"]
  [_oid ^UUID val ^DataOutputStream dos]
  (let [hi (.getMostSignificantBits val)
        lo (.getLeastSignificantBits val)]
    (doto dos
      (.writeInt 16)
      (.writeLong hi)
      (.writeLong lo))))



(defmethod -write-field [UUID "text"]
  [_oid ^UUID val ^DataOutputStream dos]
  (let [arr (-> val .toString (.getBytes const/UTF_8))]
    (doto dos
      (.writeInt (alength arr))
      (.write arr))))


#_
(comment

  (deftype Foo [^DataInputStream -dis
                ^:unsynchronized-mutable -n]

    java.util.Iterator

    (hasNext [this]
      (when-not (nil? -n)
        (set! -n (.readShort -dis)))
      (not= -n -1))

    (next [this]
      (read-line -dis -n)
      (set! -n nil)))


  (def -lines
    (with-open [in (io/input-stream "...")]
      (doall (parse-lines in ["uuid" "uuid" "jsonb"]))))

  )

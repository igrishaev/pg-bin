(def MIN_JAVA_VERSION "11")

(defproject com.github.igrishaev/pg-bin "0.1.0-SNAPSHOT"

  :description
  "Parse binary Postgres COPY output"

  :url
  "https://github.com/igrishaev/pg-bin"

  :license
  {:name "The Unlicense"
   :url "https://choosealicense.com/licenses/unlicense/"}

  :pom-addition
  [:properties
   ["maven.compiler.source" ~MIN_JAVA_VERSION]
   ["maven.compiler.target" ~MIN_JAVA_VERSION]]

  :deploy-repositories
  {"releases"
   {:url "https://repo.clojars.org"
    :creds :gpg}
   "snapshots"
   {:url "https://repo.clojars.org"
    :creds :gpg}}

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :javac-options
  ["-Xlint:unchecked"
   "-Xlint:preview"
   "--release" ~MIN_JAVA_VERSION]

  :managed-dependencies
  [[org.clojure/clojure "1.11.1"]
   [metosin/jsonista "0.3.13"]
   [cheshire "6.1.0"]
   [com.cnuernber/charred "1.037"]
   [com.github.igrishaev/jsam "0.1.0"]
   [org.clojure/data.json "2.5.1"]
   [com.github.igrishaev/taggie "0.1.1"]]

  :dependencies
  [[org.clojure/clojure :scope "provided"]]

  :profiles
  {:test
   {:resource-paths ["test/resources"]
    :dependencies
    [[metosin/jsonista]
     [cheshire]
     [com.cnuernber/charred]
     [com.github.igrishaev/jsam]
     [org.clojure/data.json]
     [com.github.igrishaev/taggie]]}})

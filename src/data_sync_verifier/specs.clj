(ns data-sync-verifier.specs
  "Data specs for data-sync-verifier (https://clojure.org/guides/spec).
  Function specs (s/fdef) live next to each defn in data_sync_verifier.core."
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as str]
            ;; a line diff's :source/:target are line texts, not the report's paths
            [data-sync-verifier.diff :as-alias diff]))

;; Generators are built inside fns, never in top-level defs:
;; clojure.spec.gen.alpha loads test.check on first use, and the JVM runtime
;; classpath (deps.edn :deps) has no test.check.

;; --- CSV ---

(defn- gen-csv-line []
  (gen/fmap (fn [fields] (str/join "," fields))
            (gen/vector (gen/one-of [(gen/elements ["id" "name" " email " "" "Alice" "a@b.com" "3"])
                                     (gen/string-alphanumeric)])
                        1 6)))

;; One line of a CSV file, without its line terminator.
(s/def ::csv-line
  (s/with-gen (s/and string? #(not (re-find #"[\r\n]" %))) gen-csv-line))
(s/def ::csv-fields (s/coll-of string? :kind vector?))

;; read-csv's result
(s/def ::headers ::csv-fields)
(s/def ::rows (s/coll-of ::csv-fields :kind vector?))
(s/def ::row-count nat-int?)
(s/def ::csv (s/keys :req-un [::headers ::rows ::row-count]))

;; A relative or absolute file path, as a string or java.nio.file.Path.
(defn- gen-path-string []
  (gen/fmap (fn [[dirs base ext]]
              (str/join "/" (conj dirs (cond-> base ext (str "." ext)))))
            (gen/tuple (gen/vector (gen/not-empty (gen/string-alphanumeric)) 0 3)
                       (gen/not-empty (gen/string-alphanumeric))
                       (gen/one-of [(gen/return nil)
                                    (gen/elements ["csv" "json" "txt"])]))))

(s/def ::path-like
  (s/with-gen (s/or :string (s/and string? seq)
                    :path #(instance? java.nio.file.Path %))
    gen-path-string))

;; --- Issues ---

(s/def ::type #{:missing-file :extra-file :checksum-failure :content-drift
                :schema-mismatch :row-count-mismatch})
(s/def ::file string?)
(s/def ::detail string?)

;; md5-checksum's output: 32 lowercase hex digits
(defn- gen-checksum []
  (gen/fmap str/join (gen/vector (gen/elements "0123456789abcdef") 32)))
(s/def ::checksum
  (s/with-gen (s/and string? #(re-matches #"[0-9a-f]{32}" %)) gen-checksum))
(s/def ::source-checksum ::checksum)
(s/def ::target-checksum ::checksum)

(s/def ::diff/line pos-int?)
(s/def ::diff/source (s/nilable string?))
(s/def ::diff/target (s/nilable string?))
(s/def ::line-diff (s/keys :req-un [::diff/line ::diff/source ::diff/target]))
(s/def ::diff-count pos-int?)
(s/def ::diffs (s/coll-of ::line-diff :kind vector? :max-count 10))

(s/def ::source-headers ::csv-fields)
(s/def ::target-headers ::csv-fields)
(s/def ::source-keys (s/coll-of string? :kind sequential?))
(s/def ::target-keys (s/coll-of string? :kind sequential?))
(s/def ::source-rows nat-int?)
(s/def ::target-rows nat-int?)

(defmulti issue-type :type)
(defmethod issue-type :missing-file [_] (s/keys :req-un [::type ::file ::detail]))
(defmethod issue-type :extra-file [_] (s/keys :req-un [::type ::file ::detail]))
(defmethod issue-type :checksum-failure [_]
  (s/keys :req-un [::type ::file ::source-checksum ::target-checksum]))
(defmethod issue-type :content-drift [_]
  (s/keys :req-un [::type ::file ::diff-count ::diffs]))
(defmethod issue-type :schema-mismatch [_]
  ;; nonconforming: conform keeps the issue map rather than tagging it
  (s/nonconforming
   (s/or :csv (s/keys :req-un [::type ::file ::source-headers ::target-headers])
         :json (s/keys :req-un [::type ::file ::source-keys ::target-keys]))))
(defmethod issue-type :row-count-mismatch [_]
  (s/keys :req-un [::type ::file ::source-rows ::target-rows]))

(s/def ::issue (s/multi-spec issue-type :type))
(s/def ::issues (s/coll-of ::issue :kind sequential? :gen-max 6))

;; --- Report ---

(s/def ::source string?)
(s/def ::target string?)
(s/def ::timestamp string?)
(s/def ::total-issues nat-int?)
(s/def ::in-sync? boolean?)
(s/def ::summary (s/map-of ::type pos-int?))
(s/def ::report
  (s/keys :req-un [::source ::target ::timestamp ::total-issues ::in-sync? ::summary ::issues]))

;; --format as typed: json and edn are recognised, anything else prints text
(s/def ::format (s/with-gen string? #(gen/elements ["text" "json" "edn" "xml"])))

;; --- CLI option table (the babashka.cli :spec map) ---

(s/def ::desc string?)
(s/def ::default string?)
(s/def ::alias simple-keyword?)
(s/def ::coerce #{:boolean :string :int :long :double :keyword :symbol})
(s/def ::cli-option (s/keys :req-un [::desc] :opt-un [::default ::alias ::coerce]))
(s/def ::cli-spec (s/map-of simple-keyword? ::cli-option))

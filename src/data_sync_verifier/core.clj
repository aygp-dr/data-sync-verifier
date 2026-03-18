(ns data_sync_verifier.core
  (:require [babashka.cli :as cli]
            [babashka.fs :as fs]
            [clojure.string :as str]
            [clojure.set :as set]
            [cheshire.core :as json]))

(def cli-spec
  {:source {:desc "Source directory or file" :alias :s}
   :target {:desc "Target directory or file" :alias :t}
   :format {:desc "Output format: text, json, edn" :default "text" :alias :f}
   :help   {:desc "Show help" :alias :h :coerce :boolean}})

;;; Checksums

(defn md5-checksum [file-path]
  (let [md     (java.security.MessageDigest/getInstance "MD5")
        bytes  (fs/read-all-bytes (fs/path file-path))
        digest (.digest md bytes)]
    (apply str (map #(format "%02x" (bit-and % 0xff)) digest))))

;;; CSV helpers

(defn parse-csv-line [line]
  (mapv str/trim (str/split line #",")))

(defn read-csv [file-path]
  (let [lines     (str/split-lines (slurp (str file-path)))
        non-empty (remove str/blank? lines)]
    (when (seq non-empty)
      {:headers   (parse-csv-line (first non-empty))
       :rows      (mapv parse-csv-line (rest non-empty))
       :row-count (max 0 (dec (count non-empty)))})))

;;; JSON helpers

(defn read-json-file [file-path]
  (json/parse-string (slurp (str file-path)) true))

;;; Directory listing

(defn relative-paths [dir]
  (let [dir-path (fs/path dir)]
    (->> (fs/glob dir "**")
         (filter fs/regular-file?)
         (map #(str (fs/relativize dir-path %)))
         (into (sorted-set)))))

;;; Content diff

(defn line-diffs [source-path target-path]
  (let [src-lines (str/split-lines (slurp (str source-path)))
        tgt-lines (str/split-lines (slurp (str target-path)))
        max-n     (max (count src-lines) (count tgt-lines))]
    (loop [i 0 diffs []]
      (if (>= i max-n)
        diffs
        (let [s (get src-lines i)
              t (get tgt-lines i)]
          (recur (inc i)
                 (if (= s t)
                   diffs
                   (conj diffs {:line (inc i) :source s :target t}))))))))

;;; File-pair checks

(defn check-file-pair [source-file target-file rel-path]
  (let [issues       (atom [])
        src-str      (str source-file)
        tgt-str      (str target-file)
        src-checksum (md5-checksum src-str)
        tgt-checksum (md5-checksum tgt-str)]
    ;; Checksum
    (when (not= src-checksum tgt-checksum)
      (swap! issues conj {:type            :checksum-failure
                          :file            rel-path
                          :source-checksum src-checksum
                          :target-checksum tgt-checksum})
      ;; Content drift
      (let [diffs (line-diffs source-file target-file)]
        (when (seq diffs)
          (swap! issues conj {:type       :content-drift
                              :file       rel-path
                              :diff-count (count diffs)
                              :diffs      (vec (take 10 diffs))}))))
    ;; CSV-specific
    (when (str/ends-with? (str/lower-case rel-path) ".csv")
      (let [src-csv (read-csv src-str)
            tgt-csv (read-csv tgt-str)]
        (when (and src-csv tgt-csv)
          (when (not= (:headers src-csv) (:headers tgt-csv))
            (swap! issues conj {:type           :schema-mismatch
                                :file           rel-path
                                :source-headers (:headers src-csv)
                                :target-headers (:headers tgt-csv)}))
          (when (not= (:row-count src-csv) (:row-count tgt-csv))
            (swap! issues conj {:type        :row-count-mismatch
                                :file        rel-path
                                :source-rows (:row-count src-csv)
                                :target-rows (:row-count tgt-csv)})))))
    ;; JSON-specific
    (when (str/ends-with? (str/lower-case rel-path) ".json")
      (try
        (let [src-json (read-json-file src-str)
              tgt-json (read-json-file tgt-str)]
          (when (and (map? src-json) (map? tgt-json))
            (let [src-keys (set (keys src-json))
                  tgt-keys (set (keys tgt-json))]
              (when (not= src-keys tgt-keys)
                (swap! issues conj {:type        :schema-mismatch
                                    :file        rel-path
                                    :source-keys (sort (map name src-keys))
                                    :target-keys (sort (map name tgt-keys))})))))
        (catch Exception _)))
    @issues))

;;; Directory comparison

(defn compare-directories [source target]
  (let [src-files (relative-paths source)
        tgt-files (relative-paths target)
        missing   (set/difference src-files tgt-files)
        extra     (set/difference tgt-files src-files)
        common    (set/intersection src-files tgt-files)
        issues    (atom [])]
    (doseq [f (sort missing)]
      (swap! issues conj {:type   :missing-file
                          :file   f
                          :detail "Present in source but missing from target"}))
    (doseq [f (sort extra)]
      (swap! issues conj {:type   :extra-file
                          :file   f
                          :detail "Present in target but not in source"}))
    (doseq [f (sort common)]
      (let [src-path    (str (fs/path source f))
            tgt-path    (str (fs/path target f))
            file-issues (check-file-pair src-path tgt-path f)]
        (swap! issues into file-issues)))
    @issues))

;;; File comparison

(defn compare-files [source target]
  (check-file-pair (str source) (str target) (str (fs/file-name (fs/path source)))))

;;; Report building

(defn build-report [source target issues]
  {:source       (str source)
   :target       (str target)
   :timestamp    (str (java.time.Instant/now))
   :total-issues (count issues)
   :in-sync?     (empty? issues)
   :summary      (into (sorted-map) (frequencies (map :type issues)))
   :issues       (vec issues)})

;;; Report formatting

(defn format-issue-text [issue]
  (case (:type issue)
    :missing-file      (format "  MISSING   %s" (:file issue))
    :extra-file        (format "  EXTRA     %s" (:file issue))
    :checksum-failure  (format "  CHECKSUM  %s  src:%s tgt:%s"
                               (:file issue)
                               (subs (:source-checksum issue) 0 8)
                               (subs (:target-checksum issue) 0 8))
    :content-drift     (format "  DRIFT     %s  (%d lines differ)"
                               (:file issue) (:diff-count issue))
    :schema-mismatch   (format "  SCHEMA    %s" (:file issue))
    :row-count-mismatch (format "  ROWCOUNT  %s  src:%d tgt:%d"
                                (:file issue) (:source-rows issue) (:target-rows issue))
    (format "  UNKNOWN   %s" (pr-str issue))))

(defn format-report [report fmt]
  (case fmt
    "json" (json/generate-string report {:pretty true})
    "edn"  (with-out-str (clojure.pprint/pprint report))
    ;; default: text
    (str/join "\n"
              (concat
               [(format "=== Data Sync Verification Report ===")
                (format "Source: %s" (:source report))
                (format "Target: %s" (:target report))
                (format "Time:   %s" (:timestamp report))
                ""
                (if (:in-sync? report)
                  "Status: IN SYNC"
                  (format "Status: OUT OF SYNC — %d issue(s) found"
                          (:total-issues report)))
                ""]
               (when (seq (:issues report))
                 (concat ["Issues:"]
                         (map format-issue-text (:issues report))
                         [""]))
               [(format "Summary: %s"
                        (if (empty? (:summary report))
                          "No issues"
                          (str/join ", "
                                   (map (fn [[k v]] (format "%s: %d" (name k) v))
                                        (:summary report)))))]))))

;;; Entry point

(defn -main [& args]
  (let [opts (cli/parse-opts args {:spec cli-spec})]
    (when (:help opts)
      (println "data-sync-verifier — Verify data integrity during sync operations")
      (println)
      (println (cli/format-opts {:spec cli-spec}))
      (System/exit 0))
    (when (or (not (:source opts)) (not (:target opts)))
      (binding [*out* *err*]
        (println "Error: --source and --target are required"))
      (System/exit 2))
    (let [source (str (:source opts))
          target (str (:target opts))
          fmt    (or (:format opts) "text")]
      (when-not (fs/exists? source)
        (binding [*out* *err*]
          (println (format "Error: source path does not exist: %s" source)))
        (System/exit 2))
      (when-not (fs/exists? target)
        (binding [*out* *err*]
          (println (format "Error: target path does not exist: %s" target)))
        (System/exit 2))
      (let [issues (cond
                     (and (fs/directory? source) (fs/directory? target))
                     (compare-directories source target)

                     (and (fs/regular-file? source) (fs/regular-file? target))
                     (compare-files source target)

                     :else
                     (do (binding [*out* *err*]
                           (println "Error: source and target must both be directories or both be files"))
                         (System/exit 2)))
            report (build-report source target issues)]
        (println (format-report report fmt))
        (System/exit (if (:in-sync? report) 0 1))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))

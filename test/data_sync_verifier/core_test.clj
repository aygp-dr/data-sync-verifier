(ns data_sync_verifier.core-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [data_sync_verifier.core :as core]
            [babashka.fs :as fs]
            [clojure.string :as str]
            [cheshire.core :as json]))

(defn with-temp-dirs
  "Create two temp dirs, call f with [source-dir target-dir], clean up after."
  [f]
  (let [src (str (fs/create-temp-dir {:prefix "dsv-src-"}))
        tgt (str (fs/create-temp-dir {:prefix "dsv-tgt-"}))]
    (try
      (f src tgt)
      (finally
        (fs/delete-tree src)
        (fs/delete-tree tgt)))))

(defn write-file! [dir rel-path content]
  (let [full-path (str (fs/path dir rel-path))]
    (fs/create-dirs (fs/parent full-path))
    (spit full-path content)))

;;; Tests

(deftest test-identical-dirs-in-sync
  (testing "Two identical directories should be in sync"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "a.txt" "hello\n")
        (write-file! tgt "a.txt" "hello\n")
        (write-file! src "sub/b.txt" "world\n")
        (write-file! tgt "sub/b.txt" "world\n")
        (let [issues (core/compare-directories src tgt)]
          (is (empty? issues) "Identical dirs should have no issues"))))))

(deftest test-missing-files
  (testing "Files in source but not target are reported as missing"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "a.txt" "hello\n")
        (write-file! src "b.txt" "world\n")
        (write-file! tgt "a.txt" "hello\n")
        (let [issues (core/compare-directories src tgt)
              types  (set (map :type issues))]
          (is (contains? types :missing-file))
          (is (= 1 (count (filter #(= :missing-file (:type %)) issues))))
          (is (= "b.txt" (:file (first (filter #(= :missing-file (:type %)) issues))))))))))

(deftest test-extra-files
  (testing "Files in target but not source are reported as extra"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "a.txt" "hello\n")
        (write-file! tgt "a.txt" "hello\n")
        (write-file! tgt "extra.txt" "surprise\n")
        (let [issues (core/compare-directories src tgt)
              extras (filter #(= :extra-file (:type %)) issues)]
          (is (= 1 (count extras)))
          (is (= "extra.txt" (:file (first extras)))))))))

(deftest test-content-drift
  (testing "Same filename with different content triggers checksum-failure and content-drift"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "data.txt" "line1\nline2\n")
        (write-file! tgt "data.txt" "line1\nline2-modified\n")
        (let [issues (core/compare-directories src tgt)
              types  (set (map :type issues))]
          (is (contains? types :checksum-failure))
          (is (contains? types :content-drift)))))))

(deftest test-csv-schema-mismatch
  (testing "CSV files with different headers trigger schema-mismatch"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "data.csv" "id,name,email\n1,Alice,a@b.com\n")
        (write-file! tgt "data.csv" "id,name,phone\n1,Alice,555-1234\n")
        (let [issues (core/compare-directories src tgt)
              schema (filter #(= :schema-mismatch (:type %)) issues)]
          (is (= 1 (count schema)))
          (is (= ["id" "name" "email"] (:source-headers (first schema))))
          (is (= ["id" "name" "phone"] (:target-headers (first schema)))))))))

(deftest test-csv-row-count-mismatch
  (testing "CSV files with different row counts trigger row-count-mismatch"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "data.csv" "id,name\n1,Alice\n2,Bob\n3,Charlie\n")
        (write-file! tgt "data.csv" "id,name\n1,Alice\n")
        (let [issues  (core/compare-directories src tgt)
              rowdiff (filter #(= :row-count-mismatch (:type %)) issues)]
          (is (= 1 (count rowdiff)))
          (is (= 3 (:source-rows (first rowdiff))))
          (is (= 1 (:target-rows (first rowdiff)))))))))

(deftest test-json-schema-mismatch
  (testing "JSON files with different top-level keys trigger schema-mismatch"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "config.json" "{\"host\": \"localhost\", \"port\": 3000}")
        (write-file! tgt "config.json" "{\"host\": \"localhost\", \"timeout\": 30}")
        (let [issues (core/compare-directories src tgt)
              schema (filter #(= :schema-mismatch (:type %)) issues)]
          (is (= 1 (count schema))))))))

(deftest test-checksum-identical-files
  (testing "Identical files produce same checksum"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "f.txt" "same content")
        (write-file! tgt "f.txt" "same content")
        (let [c1 (core/md5-checksum (str (fs/path src "f.txt")))
              c2 (core/md5-checksum (str (fs/path tgt "f.txt")))]
          (is (= c1 c2)))))))

(deftest test-compare-files-directly
  (testing "compare-files works on two individual files"
    (with-temp-dirs
      (fn [src tgt]
        (let [sf (str (fs/path src "a.csv"))
              tf (str (fs/path tgt "b.csv"))]
          (spit sf "id,name\n1,Alice\n2,Bob\n")
          (spit tf "id,name\n1,Alice\n")
          (let [issues (core/compare-files sf tf)]
            (is (some #(= :row-count-mismatch (:type %)) issues))))))))

(deftest test-report-text-format
  (testing "Text report contains expected sections"
    (let [report (core/build-report "/src" "/tgt"
                                    [{:type :missing-file :file "x.txt"
                                      :detail "Present in source but missing from target"}])
          text   (core/format-report report "text")]
      (is (str/includes? text "Data Sync Verification Report"))
      (is (str/includes? text "OUT OF SYNC"))
      (is (str/includes? text "MISSING"))
      (is (str/includes? text "x.txt")))))

(deftest test-report-json-format
  (testing "JSON report is valid JSON"
    (let [report (core/build-report "/src" "/tgt" [])
          output (core/format-report report "json")
          parsed (json/parse-string output true)]
      (is (= true (:in-sync? parsed)))
      (is (= 0 (:total-issues parsed))))))

(deftest test-report-edn-format
  (testing "EDN report is valid EDN"
    (let [report (core/build-report "/src" "/tgt" [])
          output (core/format-report report "edn")
          parsed (read-string output)]
      (is (= true (:in-sync? parsed))))))

(deftest test-mixed-issues
  (testing "A directory with multiple issue types reports all of them"
    (with-temp-dirs
      (fn [src tgt]
        (write-file! src "only-src.txt" "a")
        (write-file! tgt "only-tgt.txt" "b")
        (write-file! src "shared.txt" "version-1")
        (write-file! tgt "shared.txt" "version-2")
        (write-file! src "data.csv" "id,name\n1,A\n2,B\n")
        (write-file! tgt "data.csv" "id,email\n1,a@b\n")
        (let [issues (core/compare-directories src tgt)
              types  (set (map :type issues))]
          (is (contains? types :missing-file))
          (is (contains? types :extra-file))
          (is (contains? types :checksum-failure))
          (is (contains? types :content-drift))
          (is (contains? types :schema-mismatch))
          (is (contains? types :row-count-mismatch)))))))

(defn -main [& _args]
  (let [{:keys [fail error]} (run-tests 'data_sync_verifier.core-test)]
    (System/exit (if (pos? (+ fail error)) 1 0))))

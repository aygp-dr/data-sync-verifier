(ns data-sync-verifier.specs-test
  "Generative checks for every pure s/fdef'd fn, plus data-spec sanity.
  Per https://clojure.org/guides/spec (Testing)."
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is testing]]
            [data_sync_verifier.core :as sut]
            [data-sync-verifier.specs :as specs]))

(def ^:private check-opts {:clojure.spec.test.check/opts {:num-tests 50}})

;; Side-effecting fns (file IO, System/exit): fdef'd for instrumentation,
;; never generatively checked.
(def ^:private side-effecting
  #{`sut/md5-checksum `sut/read-csv `sut/read-json-file `sut/relative-paths
    `sut/line-diffs `sut/check-file-pair `sut/compare-directories
    `sut/compare-files `sut/-main})

(defn- checkable []
  (remove side-effecting (stest/enumerate-namespace 'data_sync_verifier.core)))

(deftest fdefs-hold-under-generative-testing
  (let [results (stest/check (checkable) check-opts)]
    (is (seq results) "expected at least one fdef'd fn to check")
    (doseq [r results]
      (testing (str (:sym r))
        (is (nil? (:failure r))
            (pr-str (stest/abbrev-result r)))))))

(deftest data-specs-generate-and-conform
  (doseq [k [::specs/csv-line ::specs/path-like ::specs/issue ::specs/issues
             ::specs/report ::specs/cli-spec]]
    (testing (str k)
      (is (every? (fn [[v _]] (s/valid? k v)) (s/exercise k 10))))))

(def ^:private fixture-issues
  ;; shapes taken from core_test's scenarios
  [{:type :missing-file :file "b.txt" :detail "Present in source but missing from target"}
   {:type :checksum-failure :file "shared.txt"
    :source-checksum "5d41402abc4b2a76b9719d911017c592"
    :target-checksum "7d793037a0760186574b0282f2f435e7"}
   {:type :content-drift :file "shared.txt" :diff-count 1
    :diffs [{:line 1 :source "version-1" :target "version-2"}]}
   {:type :schema-mismatch :file "data.csv"
    :source-headers ["id" "name"] :target-headers ["id" "email"]}
   {:type :schema-mismatch :file "config.json"
    :source-keys ["host" "port"] :target-keys ["host" "timeout"]}
   {:type :row-count-mismatch :file "data.csv" :source-rows 2 :target-rows 1}])

(deftest real-values-conform
  (testing "the option table"
    (is (s/valid? ::specs/cli-spec sut/cli-spec)))
  (testing "issue shapes from the unit tests"
    (is (s/valid? ::specs/issues fixture-issues))
    (is (s/valid? ::specs/report (sut/build-report "/src" "/tgt" fixture-issues))))
  (testing "a parsed CSV header"
    (is (s/valid? ::specs/csv-fields (sut/parse-csv-line "id, name ,email")))))

(ns xtdb.google.cloud-storage-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.fixtures.checkpoint-store :as fix.cp]
            [xtdb.google.cloud-storage :as gcs]
            [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import [java.util Date UUID]))

(def test-bucket
  (System/getProperty "xtdb.google.cloud-storage-test.bucket"))

(t/use-fixtures :once
  (fn [f]
    (when test-bucket
      (f))))

(t/deftest test-doc-store
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:root-path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::gcs/document-store sys))))

(t/deftest test-cp-store
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.cp/test-checkpoint-store (::gcs/checkpoint-store sys))))

(t/deftest test-checkpoint-store-cleanup
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:root-path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (::gcs/checkpoint-store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                      :tx {::xt/tx-id 1}
                                                                      :cp-at cp-at})]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (t/is (.exists (io/file cp-uri)))
          (t/is (.exists (io/file (str cp-uri ".edn")))))

        (t/testing "call to `cleanup-checkpoints` entirely removes an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (= false (.exists (io/file cp-uri))))
          (t/is (= false (.exists (io/file (str cp-uri ".edn"))))))))))

(t/deftest test-checkpoint-store-failed-cleanup
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:root-path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (::gcs/checkpoint-store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                      :tx {::xt/tx-id 1}
                                                                      :cp-at cp-at})]

        (t/testing "error in `cleanup-checkpoints` after deleting checkpoint metadata file still leads to checkpoint not being available"
          (with-redefs [xio/delete-dir (fn [_] (throw (Exception. "Test Exception")))]
            (t/is (thrown-with-msg? Exception
                                    #"Test Exception"
                                    (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                                                     :cp-at cp-at}))))
          ;; Only directory should be available - checkpoint metadata file should have been deleted
          (t/is (.exists (io/file cp-uri)))
          (t/is (= false (.exists (io/file (str cp-uri ".edn")))))
          ;; Should not be able to fetch checkpoint as checkpoint metadata file is gone
          (t/is (empty? (cp/available-checkpoints cp-store ::foo-cp-format))))))))

(t/deftest test-checkpoint-store-cleanup-no-edn-file
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:root-path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (::gcs/checkpoint-store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                      :tx {::xt/tx-id 1}
                                                                      :cp-at cp-at})]
        ;; delete the checkpoint file
        (.delete (io/file (str cp-uri ".edn")))

        (t/testing "checkpoint files present, edn file should be deleted"
          (t/is (.exists (io/file cp-uri)))
          (t/is (= false (.exists (io/file (str cp-uri ".edn"))))))

        (t/testing "call to `cleanup-checkpoints` with no edn file should still remove an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (= false (.exists (io/file cp-uri))))
          (t/is (= false (.exists (io/file (str cp-uri ".edn"))))))))))

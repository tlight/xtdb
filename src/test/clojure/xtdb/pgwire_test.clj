(ns xtdb.pgwire-test
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing] :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.protocols :as xtp]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgwire]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (:import (com.fasterxml.jackson.databind JsonNode ObjectMapper)
           (com.fasterxml.jackson.databind.node JsonNodeType)
           (java.lang Thread$State)
           (java.net SocketException)
           (java.sql Connection PreparedStatement)
           (java.time Clock Instant ZoneId ZoneOffset)
           (java.util.concurrent CompletableFuture CountDownLatch TimeUnit)
           java.util.stream.StreamSupport
           #_org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
           (org.postgresql.util PGobject PSQLException)
           xtdb.ICursor))

(set! *warn-on-reflection* false)
(set! *unchecked-math* false)

(def ^:dynamic ^:private *port* nil)
(def ^:dynamic ^:private *node* nil)
(def ^:dynamic ^:private *server* nil)

(defn require-node []
  (when-not *node*
    (set! *node* (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]}))))

(defn require-server
  ([] (require-server {}))
  ([opts]
   (require-node)
   (when-not *port*
     (set! *port* (tu/free-port))
     (->> (merge {:num-threads 1}
                 opts
                 {:port *port*})
          (pgwire/serve *node*)
          (set! *server*)))))

(t/use-fixtures :once

  #_ ; HACK commented out while we're not bringing in the Flight JDBC driver
  (fn [f]
    ;; HACK see https://issues.apache.org/jira/browse/ARROW-18296
    ;; this ensures the FSQL driver is at the end of the DriverManager list
    (when-let [fsql-driver (->> (enumeration-seq (DriverManager/getDrivers))
                                (filter #(instance? ArrowFlightJdbcDriver %))
                                first)]
      (DriverManager/deregisterDriver fsql-driver)
      (DriverManager/registerDriver fsql-driver))
    (f)))

(t/use-fixtures :each
  (fn [f]
    (binding [*port* nil
              *server* nil
              *node* nil]
      (try
        (f)
        (finally
          (util/try-close *server*)
          (util/try-close *node*))))))

(defn- jdbc-url [& params]
  (require-server)
  (assert *port* "*port* must be bound")
  (let [param-str (when (seq params) (str "?" (str/join "&" (for [[k v] (partition 2 params)] (str k "=" v)))))]
    (format "jdbc:postgresql://:%s/xtdb%s" *port* param-str)))

(deftest connect-with-next-jdbc-test
  (with-open [_ (jdbc/get-connection (jdbc-url))])
  ;; connect a second time to make sure we are releasing server resources properly!
  (with-open [_ (jdbc/get-connection (jdbc-url))]))

(defn- try-sslmode [sslmode]
  (try
    (with-open [_ (jdbc/get-connection (jdbc-url "sslmode" sslmode))])
    :ok
    (catch PSQLException e
      (if (= "The server does not support SSL." (.getMessage e))
        :unsupported
        (throw e)))))

(deftest ssl-test
  (t/are [sslmode expect] (= expect (try-sslmode sslmode))
    "disable" :ok
    "allow" :ok
    "prefer" :ok

    "require" :unsupported
    "verify-ca" :unsupported
    "verify-full" :unsupported))

(defn- try-gssencmode [gssencmode]
  (try
    (with-open [_ (jdbc/get-connection (jdbc-url "gssEncMode" gssencmode))])
    :ok
    (catch PSQLException e
      (if (= "The server does not support GSS Encoding." (.getMessage e))
        :unsupported
        (throw e)))))

(deftest gssenc-test
  (t/are [gssencmode expect]
    (= expect (try-gssencmode gssencmode))

    "disable" :ok
    "prefer" :ok
    "require" :unsupported))

(defn- jdbc-conn ^Connection [& params]
  (jdbc/get-connection (apply jdbc-url params)))

(deftest query-test
  (with-open [conn (jdbc-conn)
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

(deftest simple-query-test
  (with-open [conn (jdbc-conn "preferQueryMode" "simple")
              stmt (.createStatement conn)
              rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

(deftest prepared-query-test
  (with-open [conn (jdbc-conn "prepareThreshold" "1")
              stmt (.prepareStatement conn "SELECT a.a FROM (VALUES ('hello, world')) a (a)")
              stmt2 (.prepareStatement conn "SELECT a.a FROM (VALUES ('hello, world2')) a (a)")]

    (with-open [rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))

    (with-open [rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))

    ;; exec queries a few times to trigger .execute prepared statements in jdbc

    (dotimes [_ 5]
      (with-open [rs (.executeQuery stmt2)]
        (is (= true (.next rs)))
        (is (= "\"hello, world2\"" (str (.getObject rs 1))))
        (is (= false (.next rs)))))

    (dotimes [_ 5]
      (with-open [rs (.executeQuery stmt)]
        (is (= true (.next rs)))
        (is (= "\"hello, world\"" (str (.getObject rs 1))))
        (is (= false (.next rs)))))))

(deftest parameterized-query-test
  (with-open [conn (jdbc-conn)
              stmt (doto (.prepareStatement conn "SELECT a.a FROM (VALUES (?)) a (a)")
                     (.setObject 1 "hello, world"))
              rs (.executeQuery stmt)]
    (is (= true (.next rs)))
    (is (= false (.next rs)))))

(def json-representation-examples
  "A map of entries describing sql value domains
  and properties of their json representation.

  :sql the SQL expression that produces the value
  :json-type the expected Jackson JsonNodeType

  :json (optional) a json string that we expect back from pgwire
  :clj (optional) a clj value that we expect from clojure.data.json/read-str
  :clj-pred (optional) a fn that returns true if the parsed arg (via data.json/read-str) is what we expect"
  (letfn [(string [s]
            {:sql (str "'" s "'")
             :json-type JsonNodeType/STRING
             :clj s})
          (integer [i]
            {:sql (str i)
             :json-type JsonNodeType/NUMBER
             :clj-pred #(= (bigint %) (bigint i))})
          (decimal [n & {:keys [add-zero]}]
            (let [d1 (bigdec n)
                  d2 (if add-zero (.setScale d1 1) d1)]
              {:sql (.toPlainString d2)
               :json-type JsonNodeType/NUMBER
               :json (str d1)
               :clj-pred #(= (bigdec %) (bigdec n))}))]

    [{:sql "null"
      :json-type JsonNodeType/NULL
      :clj nil}

     {:sql "true"
      :json-type JsonNodeType/BOOLEAN
      :clj true}

     (string "hello, world")
     (string "")
     (string "42")
     (string "2022-01-03")

     ;; numbers

     (integer 0)
     (integer -0)
     (integer 42)
     (integer Long/MAX_VALUE)

     (integer Long/MIN_VALUE)

     (decimal 0.0)
     (decimal -0.0)
     (decimal 3.14)
     (decimal 42.0)

     ;; does not work no exact decimal support currently
     (decimal Double/MIN_VALUE)
     (decimal Double/MAX_VALUE :add-zero true)

     ;; dates / times

     {:sql "DATE '2021-12-24'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24"}
     {:sql "TIMESTAMP '2021-03-04 03:04:11'"
      :json-type JsonNodeType/STRING
      :clj "2021-03-04T03:04:11"}
     {:sql "TIMESTAMP '2021-03-04 03:04:11+02:00'"
      :json-type JsonNodeType/STRING
      :clj "2021-03-04T03:04:11+02:00"}
     {:sql "TIMESTAMP '2021-12-24 11:23:44.003'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24T11:23:44.003"}

     {:sql "1 YEAR"
      :json-type JsonNodeType/STRING
      :clj "P12M"}
     {:sql "1 MONTH"
      :json-type JsonNodeType/STRING
      :clj "P1M"}

     {:sql "DATE '2021-12-24' - DATE '2021-12-23'"
      :json-type JsonNodeType/NUMBER
      :clj 1}

     ;; arrays

     {:sql "ARRAY []"
      :json-type JsonNodeType/ARRAY
      :clj []}

     {:sql "ARRAY [42]"
      :json-type JsonNodeType/ARRAY
      :clj [42]}

     {:sql "ARRAY ['2022-01-02']"
      :json-type JsonNodeType/ARRAY
      :json "[\"2022-01-02\"]"
      :clj ["2022-01-02"]}

     {:sql "ARRAY [ARRAY ['42'], 42, '42']"
      :json-type JsonNodeType/ARRAY
      :clj [["42"] 42 "42"]}]))

(deftest json-representation-test
  (with-open [conn (jdbc-conn)]
    (doseq [{:keys [json-type, json, sql, clj, clj-pred] :as example} json-representation-examples]
      (testing (str "SQL expression " sql " should parse to " clj " (" (when json (str json ", ")) json-type ")")
        (with-open [stmt (.prepareStatement conn (format "SELECT %s FROM (VALUES (1)) a (a)" sql))]
          (with-open [rs (.executeQuery stmt)]
            ;; one row in result set
            (.next rs)

            (testing "record set contains expected object"
              (is (instance? PGobject (.getObject rs 1)))
              (is (= "json" (.getType ^PGobject (.getObject rs 1)))))

            (testing (str "json parses to " (str json-type))
              (let [obj-mapper (ObjectMapper.)
                    json-str (str (.getObject rs 1))
                    ^JsonNode read-value (.readValue obj-mapper json-str ^Class JsonNode)]
                ;; use strings to get a better report
                (is (= (str json-type) (str (.getNodeType read-value))))
                (when json
                  (is (= json json-str) "json string should be = to :json"))))

            (testing "json parses to expected clj value"
              (let [clj-value (json/read-str (str (.getObject rs 1)))]
                (when (contains? example :clj)
                  (is (= clj clj-value) "parsed value should = :clj"))
                (when clj-pred
                  (is (clj-pred clj-value) "parsed value should pass :clj-pred"))))))))))

(defn check-server-resources-freed
  ([]
   (require-server)
   (check-server-resources-freed *server*))
  ([server]
   (testing "accept socket"
     (is (.isClosed @(:accept-socket server))))

   (testing "accept thread"
     (is (= Thread$State/TERMINATED (.getState (:accept-thread server)))))

   (testing "thread pool shutdown"
     (is (.isShutdown (:thread-pool server)))
     (is (.isTerminated (:thread-pool server))))))

(deftest server-resources-freed-on-close-test
  (require-node)
  (doseq [close-method [#(.close %)]]
    (with-open [server (pgwire/serve *node* {:port (tu/free-port)})]
      (close-method server)
      (check-server-resources-freed server))))

(deftest server-resources-freed-if-exc-on-start-test
  (require-node)
  (with-open [server (pgwire/serve *node* {:port (tu/free-port)
                                           :unsafe-init-state
                                           {:silent-start true
                                            :injected-start-exc (Exception. "boom!")}})]
    (check-server-resources-freed server)))

;; #673
#_(deftest accept-thread-and-socket-closed-on-uncaught-accept-exc-test
  (require-server)

  (swap! (:server-state *server*) assoc
         :injected-accept-exc (Exception. "boom")
         :silent-accept true)

  (is (thrown? Throwable (with-open [_ (jdbc-conn)])))

  (testing "registered"
    (is (registered? *server*)))

  (testing "accept socket"
    (is (.isClosed @(:accept-socket *server*))))

  (testing "accept thread"
    (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*))))))

(defn q [conn sql]
  (->> (jdbc/execute! conn sql)
       (mapv (fn [row] (update-vals row (comp json/read-str str))))))

(defn ping [conn]
  (-> (q conn ["select a.ping from (values ('pong')) a (ping)"])
      first
      :ping))

(defn- inject-accept-exc
  ([]
   (inject-accept-exc (Exception. "")))
  ([ex]
   (require-server)
   (swap! (:server-state *server*)
          assoc :injected-accept-exc ex, :silent-accept true)
   nil))

(defn- connect-and-throwaway []
  (try (jdbc-conn) (catch Throwable _)))

(deftest accept-uncaught-exception-allows-free-test
  (inject-accept-exc)
  (connect-and-throwaway)
  (.close *server*)
  (check-server-resources-freed))

;; #673
#_(deftest accept-thread-stoppage-sets-error-status
  (inject-accept-exc)
  (connect-and-throwaway)
  (is (= :error @(:server-status *server*))))

(deftest accept-thread-stoppage-allows-other-conns-to-continue-test
  (with-open [conn1 (jdbc-conn)]
    (inject-accept-exc)
    (connect-and-throwaway)
    (is (= "pong" (ping conn1)))))

(deftest accept-thread-socket-closed-exc-does-not-stop-later-accepts-test
  (inject-accept-exc (SocketException. "Socket closed"))
  (connect-and-throwaway)
  (is (with-open [_conn (jdbc-conn)] true)))

(deftest accept-thread-interrupt-closes-thread-test
  (require-server {:accept-so-timeout 10})

  (.interrupt (:accept-thread *server*))
  (.join (:accept-thread *server*) 1000)

  (is (:accept-interrupted @(:server-state *server*)))
  (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*)))))

(deftest accept-thread-interrupt-allows-server-shutdown-test
  (require-server {:accept-so-timeout 10})

  (.interrupt (:accept-thread *server*))
  (.join (:accept-thread *server*) 1000)

  (.close *server*)
  (check-server-resources-freed))

(deftest accept-thread-socket-close-stops-thread-test
  (require-server)
  (.close @(:accept-socket *server*))
  (.join (:accept-thread *server*) 1000)
  (is (= Thread$State/TERMINATED (.getState (:accept-thread *server*)))))

(deftest accept-thread-socket-close-allows-cleanup-test
  (require-server)
  (.close @(:accept-socket *server*))
  (.join (:accept-thread *server*) 1000)
  (.close *server*)
  (check-server-resources-freed))

(deftest accept-socket-timeout-set-by-default-test
  (require-server)
  (is (pos? (.getSoTimeout @(:accept-socket *server*)))))

(deftest accept-socket-timeout-can-be-unset-test
  (require-server {:accept-so-timeout nil})
  (is (= 0 (.getSoTimeout @(:accept-socket *server*)))))

(defn- get-connections []
  (vals (:connections @(:server-state *server*))))

(defn- get-last-conn []
  (last (sort-by :cid (get-connections))))

(defn- wait-for-close [server-conn ms]
  (deref (:close-promise @(:conn-state server-conn)) ms false))

(defn check-conn-resources-freed [server-conn]
  (let [{:keys [socket]} server-conn]
    (t/is (.isClosed socket))))

(deftest conn-force-closed-by-server-frees-resources-test
  (require-server)
  (with-open [_ (jdbc-conn)]
    (let [srv-conn (get-last-conn)]
      (.close srv-conn)
      (check-conn-resources-freed srv-conn))))

(deftest conn-closed-by-client-frees-resources-test
  (require-server)
  (with-open [client-conn (jdbc-conn)
              server-conn (get-last-conn)]
    (.close client-conn)
    (is (wait-for-close server-conn 500))
    (check-conn-resources-freed server-conn)))

(deftest server-close-closes-idle-conns-test
  (require-server {:drain-wait 0})
  (with-open [_client-conn (jdbc-conn)
              server-conn (get-last-conn)]
    (.close *server*)
    (is (wait-for-close server-conn 500))
    (check-conn-resources-freed server-conn)))

(deftest canned-response-test
  (require-server)
  ;; quick test for now to confirm canned response mechanism at least doesn't crash!
  ;; this may later be replaced by client driver tests (e.g test sqlalchemy connect & query)
  (with-redefs [pgwire/canned-responses [{:q "hello!"
                                          :cols [{:column-name "greet", :column-oid @#'pgwire/oid-json}]
                                          :rows [["\"hey!\""]]}]]
    (with-open [conn (jdbc-conn)]
      (is (= [{:greet "hey!"}] (q conn ["hello!"]))))))

(deftest concurrent-conns-test
  (require-server {:num-threads 2})
  (let [results (atom [])
        spawn (fn spawn []
                (future
                  (with-open [conn (jdbc-conn)]
                    (swap! results conj (ping conn)))))
        futs (vec (repeatedly 10 spawn))]

    (is (every? #(not= :timeout (deref % 500 :timeout)) futs))
    (is (= 10 (count @results)))

    (.close *server*)
    (check-server-resources-freed)))

(deftest concurrent-conns-close-midway-test
  (require-server {:num-threads 2
                   :accept-so-timeout 10})
  (tu/with-log-level 'xtdb.pgwire :off
    (let [spawn (fn spawn [i]
                  (future
                    (try
                      (with-open [conn (jdbc-conn "loginTimeout" "1"
                                                  "socketTimeout" "1")]
                        (loop [query-til (+ (System/currentTimeMillis)
                                            (* i 1000))]
                          (ping conn)
                          (when (< (System/currentTimeMillis) query-til)
                            (recur query-til))))
                      ;; we expect an ex here, whether or not draining
                      (catch PSQLException _))))

          futs (mapv spawn (range 10))]

      (is (some #(not= :timeout (deref % 1000 :timeout)) futs))

      (.close *server*)

      (is (every? #(not= :timeout (deref % 1000 :timeout)) futs))

      (check-server-resources-freed))))

;; the goal of this test is to cause a bunch of ping queries to block on parse
;; until the server is draining
;; and observe that connection continue until the multi-message extended interaction is done
;; (when we introduce read transactions I will probably extend this to short-lived transactions)
(deftest close-drains-active-extended-queries-before-stopping-test
  (require-server {:num-threads 10
                   :accept-so-timeout 10})
  (let [cmd-parse @#'pgwire/cmd-parse
        server-status (:server-status *server*)
        latch (CountDownLatch. 10)]
    ;; redefine parse to block when we ping
    (with-redefs [pgwire/cmd-parse
                  (fn [conn {:keys [query] :as cmd}]
                    (if-not (str/starts-with? query "select a.ping")
                      (cmd-parse conn cmd)
                      (do
                        (.countDown latch)
                        ;; delay until we see a draining state
                        (loop [wait-until (+ (System/currentTimeMillis) 5000)]
                          (when (and (< (System/currentTimeMillis) wait-until)
                                     (not= :draining @server-status))
                            (recur wait-until)))
                        (cmd-parse conn cmd))))]
      (let [spawn (fn spawn [] (future (with-open [conn (jdbc-conn)] (ping conn))))
            futs (vec (repeatedly 10 spawn))]

        (is (.await latch 1 TimeUnit/SECONDS))

        (.close *server*)

        (is (every? #(= "pong" (deref % 1000 :timeout)) futs))

        (check-server-resources-freed)))))

(deftest jdbc-query-cancellation-test
  (require-server {:num-threads 2})
  (let [stmt-promise (promise)

        start-conn1
        (fn []
          (with-open [conn (jdbc-conn)
                      stmt (.prepareStatement conn "select a.a from a")]
            (try
              (with-redefs [pgwire/open-query& (fn [& _] (deliver stmt-promise stmt) (CompletableFuture.))]
                (with-open [_rs (.executeQuery stmt)]
                  :not-cancelled))
              (catch PSQLException e
                (.getMessage e)))))

        fut (future (start-conn1))]

    (is (not= :timeout (deref stmt-promise 1000 :timeout)))
    (when (realized? stmt-promise) (.cancel ^PreparedStatement @stmt-promise))
    (is (= "ERROR: query cancelled during execution" (deref fut 1000 :timeout)))))

(deftest jdbc-prepared-query-close-test
  (with-open [conn (jdbc-conn "prepareThreshold" "1"
                              "preparedStatementCacheQueries" 0
                              "preparedStatementCacheMiB" 0)]
    (dotimes [i 3]
      ;; do not use parameters as to trigger close it needs to be a different query every time
      (with-open [stmt (.prepareStatement conn (format "SELECT a.a FROM (VALUES (%s)) a (a)" i))]
        (.close (.executeQuery stmt))))

    (testing "only empty portal should remain"
      (is (= [""] (keys (:portals @(:conn-state (get-last-conn)))))))

    (testing "even at cache policy 0, pg jdbc caches - but we should only see the last stmt + empty"
      ;; S_3 because i == 3
      (is (= #{"", "S_3"} (set (keys (:prepared-statements @(:conn-state (get-last-conn))))))))))

(defn psql-available?
  "Returns true if psql is available in $PATH"
  []
  (try (= 0 (:exit (sh/sh "command" "-v" "psql"))) (catch Throwable _ false)))

(defn psql-session
  "Takes a function of two args (send, read).

  Send puts a string in to psql stdin, reads the next string from psql stdout. You can use (read :err) if you wish to read from stderr instead."
  [f]
  (require-server)
  ;; there are other ways to do this, but its a straightforward factoring that removes some boilerplate for now.
  (let [pb (ProcessBuilder. ["psql" "-h" "localhost" "-p" (str *port*)])
        p (.start pb)
        in (delay (.getInputStream p))
        err (delay (.getErrorStream p))
        out (delay (.getOutputStream p))

        send
        (fn [s]
          (.write @out (.getBytes s "utf-8"))
          (.flush @out))

        read
        (fn read
          ([] (read @in))
          ([stream]
           (let [stream (case stream :err @err stream)]
             (loop [wait-until (+ (System/currentTimeMillis) 1000)]
               (cond
                 (pos? (.available stream))
                 (let [barr (byte-array (.available stream))]
                   (.read stream barr)
                   (String. barr))

                 (< wait-until (System/currentTimeMillis)) :timeout
                 :else (recur wait-until))))))]
    (try
      (f send read)
      (finally
        (when (.isAlive p)
          (.destroy p))

        (is (.waitFor p 1000 TimeUnit/MILLISECONDS))
        (is (#{143, 0} (.exitValue p)))

        (when (realized? in) (util/try-close @in))
        (when (realized? out) (util/try-close @out))
        (when (realized? err) (util/try-close @err))))))

;; define psql tests if psql is available on path
;; (will probably move to a selector)
(when (psql-available?)

  (deftest psql-connect-test
    (require-server)
    (let [{:keys [exit, out]} (sh/sh "psql" "-h" "localhost" "-p" (str *port*) "-c" "select ping")]
      (is (= 0 exit))
      (is (str/includes? out " pong\n(1 row)"))))

  (deftest psql-interactive-test
    (psql-session
      (fn [send read]
        (testing "ping"
          (send "select ping;\n")
          (let [s (read)]
            (is (str/includes? s "pong"))
            (is (str/includes? s "(1 row)"))))

        (testing "numeric printing"
          (send "select a.a from (values (42)) a (a);\n")
          (is (str/includes? (read) "42")))

        (testing "expecting column name"
          (send "select a.flibble from (values (42)) a (flibble);\n")
          (is (str/includes? (read) "flibble")))

        (testing "mixed type col"
          (send "select a.a from (values (42), ('hello!'), (array [1,2,3])) a (a);\n")
          (let [s (read)]
            (is (str/includes? s "42"))
            (is (str/includes? s "\"hello!\""))
            (is (str/includes? s "[1,2,3]"))
            (is (str/includes? s "(3 rows)"))))

        (testing "parse error"
          (send "not really sql;\n")
          (is (str/includes? (read :err) "ERROR"))

          (testing "parse error allows session to continue"
            (send "select ping;\n")
            (is (str/includes? (read) "pong"))))

        (testing "query crash during plan"
          (with-redefs [clojure.tools.logging/logf (constantly nil)
                        xtp/open-query& (fn [& _] (CompletableFuture/failedFuture (Throwable. "oops")))]
            (send "select a.a from (values (42)) a (a);\n")
            (is (str/includes? (read :err) "unexpected server error during query execution")))

          (testing "internal query error allows session to continue"
            (send "select ping;\n")
            (is (str/includes? (read) "pong"))))

        (testing "query crash during result set iteration"
          (with-redefs [clojure.tools.logging/logf (constantly nil)
                        xtp/open-query& (fn [& _]
                                          (CompletableFuture/completedFuture
                                           (StreamSupport/stream
                                            (reify ICursor
                                              (tryAdvance [_ _c]
                                                (throw (Throwable. "oops"))))
                                            false)))]
            (send "select a.a from (values (42)) a (a);\n")
            (is (str/includes? (read :err) "unexpected server error during query execution")))

          (testing "internal query error allows session to continue"
            (send "select ping;\n")
            (is (str/includes? (read) "pong"))))))))

(def pg-param-representation-examples
  "A library of examples to test pg parameter oid handling.

  e.g set this object as a param, round trip it - does it match the json result?

  :param (the java object, e.g (int 42))
  :json the expected (parsed via data.json) json representation
  :json-cast (a function to apply to the json, useful for downcast for floats)"
  [{:param nil
    :json nil}

   {:param true
    :json true}

   {:param false
    :json false}

   {:param (byte 42)
    :json 42}

   {:param (byte -42)
    :json -42}

   {:param (short 257)
    :json 257}

   {:param (short -257)
    :json -257}

   {:param (int 92767)
    :json 92767}

   {:param (int -92767)
    :json -92767}

   {:param (long 4147483647)
    :json 4147483647}

   {:param (long -4147483647)
    :json -4147483647}

   {:param (float Math/PI)
    :json (float Math/PI)
    :json-cast float}

   {:param (+ 1.0 (double Float/MAX_VALUE))
    :json (+ 1.0 (double Float/MAX_VALUE))}

   {:param ""
    :json ""}

   {:param "hello, world!"
    :json "hello, world!"}

   {:param "😎"
    :json "😎"}])

(deftest pg-param-representation-test
  (with-open [conn (jdbc-conn)
              stmt (.prepareStatement conn "select a.a from (values (?)) a (a)")]
    (doseq [{:keys [param, json, json-cast]
             :or {json-cast identity}}
            pg-param-representation-examples]
      (testing (format "param %s (%s)" param (class param))

        (.clearParameters stmt)

        (condp instance? param
          Byte (.setByte stmt 1 param)
          Short (.setShort stmt 1 param)
          Integer (.setInt stmt 1 param)
          Long (.setLong stmt 1 param)
          Float (.setFloat stmt 1 param)
          Double (.setDouble stmt 1 param)
          String (.setString stmt 1 param)
          (.setObject stmt 1 param))

        (with-open [rs (.executeQuery stmt)]
          (is (.next rs))
          ;; may want more fine-grained json assertions than this, it depends on data.json behaviour
          (is (= json (json-cast (json/read-str (str (.getObject rs 1)))))))))))

;; maps cannot be created from SQL yet, or used as parameters - but we can read them from XT.
(deftest map-read-test
  (with-open [conn (jdbc-conn)]
    (-> (xt/submit-tx *node* [[:put-docs :a {:xt/id "map-test", :a {:b 42}}]])
        (tu/then-await-tx *node*))

    (let [rs (q conn ["select a.a from a a"])]
      (is (= [{:a {"b" 42}}] rs)))))

(deftest start-stop-as-module-test
  (tu/with-log-level 'xtdb.pgwire :info
    (let [port (tu/free-port)]
      (with-open [_node (xtn/start-node {:pgwire-server {:port port
                                                         :num-threads 3}})
                  conn (jdbc-conn)]
        (is (= "pong" (ping conn)))))))

(deftest open-close-transaction-does-not-crash-test
  (with-open [conn (jdbc-conn)]
    (jdbc/with-transaction [db conn]
      (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"]))))))

;; for now, behaviour will change later I am sure
(deftest different-transaction-isolation-levels-accepted-and-ignored-test
  (with-open [conn (jdbc-conn)]
    (doseq [level [:read-committed
                   :read-uncommitted
                   :repeatable-read
                   :serializable]]
      (testing (format "can open and close transaction (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level}]
          (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"])))))
      (testing (format "readonly accepted (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level, :read-only true}]
          (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"])))))
      (testing (format "rollback only accepted (%s)" level)
        (jdbc/with-transaction [db conn {:isolation level, :rollback-only true}]
          (is (= [{:ping "pong"}] (jdbc/execute! db ["select ping;"]))))))))

;; right now all isolation levels have the same defined behaviour
(deftest transaction-by-default-pins-the-basis-to-last-tx-test
  (require-node)
  (let [insert #(xt/submit-tx *node* [[:put-docs %1 %2]])]
    (-> (insert :a {:xt/id :fred, :name "Fred"})
        (tu/then-await-tx *node*))

    (with-open [conn (jdbc-conn)]
      (jdbc/with-transaction [db conn]
        (is (= [{:name "Fred"}] (q db ["select a.name from a"])))
        (insert :a {:xt/id :bob, :name "Bob"})
        (is (= [{:name "Fred"}] (q db ["select a.name from a"]))))

      (is (= #{{:name "Fred"}, {:name "Bob"}} (set (q conn ["select a.name from a"])))))))

;; SET is not supported properly at the moment, so this ensure we do not really do anything too embarrassing (like crash)
(deftest set-statement-test
  (let [params #(-> (get-last-conn) :conn-state deref :session :parameters (select-keys %))]
    (with-open [conn (jdbc-conn)]

      (testing "literals saved as is"
        (is (q conn ["SET a = 'b'"]))
        (is (= {:a "b"} (params [:a])))
        (is (q conn ["SET b = 42"]))
        (is (= {:a "b", :b 42} (params [:a :b]))))

      (testing "properties can be overwritten"
        (q conn ["SET a = 42"])
        (is (= {:a 42} (params [:a]))))

      (testing "TO syntax can be used"
        (q conn ["SET a TO 43"])
        (is (= {:a 43} (params [:a])))))))

(deftest db-queryable-after-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (try
      (jdbc/with-transaction [db conn]
        (is (= [] (q db ["select a.a from a a"])))
        (throw (Exception. "Oh no!")))
      (catch Throwable _))
    (is (= [] (q conn ["select a.a from a a"])))))

(deftest transactions-are-read-only-by-default-test
  (with-open [conn (jdbc-conn)]
    (is (thrown-with-msg?
          PSQLException #"ERROR\: DML is not allowed in a READ ONLY transaction"
          (jdbc/with-transaction [db conn] (q db ["insert into foo(xt$id) values (42)"]))))
    (is (= [] (q conn ["select foo.a from foo foo"])))))

(defn- session-variables [server-conn ks]
  (-> server-conn :conn-state deref :session (select-keys ks)))

(defn- next-transaction-variables [server-conn ks]
  (-> server-conn :conn-state deref :session :next-transaction (select-keys ks)))

(deftest session-access-mode-default-test
  (require-node)
  (with-open [_ (jdbc-conn)]
    (is (= {:access-mode :read-only} (session-variables (get-last-conn) [:access-mode])))))

(deftest set-transaction-test
  (with-open [conn (jdbc-conn)]
    (testing "SET TRANSACTION overwrites variables for the next transaction"
      (q conn ["SET TRANSACTION READ ONLY"])
      (is (= {:access-mode :read-only} (next-transaction-variables (get-last-conn) [:access-mode])))
      (q conn ["SET TRANSACTION READ WRITE"])
      (is (= {:access-mode :read-write} (next-transaction-variables (get-last-conn) [:access-mode]))))

    (testing "opening and closing a transaction clears this state"
      (q conn ["SET TRANSACTION READ ONLY"])
      (jdbc/with-transaction [tx conn] (ping tx))
      (is (= {} (next-transaction-variables (get-last-conn) [:access-mode])))
      (is (= {:access-mode :read-only} (session-variables (get-last-conn) [:access-mode]))))

    (testing "rolling back a transaction clears this state"
      (q conn ["SET TRANSACTION READ WRITE"])
      (.setAutoCommit conn false)
      ;; explicitly send rollback .rollback doesn't necessarily do it if you haven't issued a query
      (q conn ["ROLLBACK"])
      (.setAutoCommit conn true)
      (is (= {} (next-transaction-variables (get-last-conn) [:access-mode]))))))

(defn tx! [conn & sql]
  (q conn ["SET TRANSACTION READ WRITE"])
  (jdbc/with-transaction [tx conn]
    (run! #(q tx %) sql)))

(deftest dml-test
  (with-open [conn (jdbc-conn)]
    (testing "mixing a read causes rollback"
      (q conn ["SET TRANSACTION READ WRITE"])
      (is (thrown-with-msg? PSQLException #"queries are unsupported in a READ WRITE transaction"
                            (jdbc/with-transaction [tx conn]
                              (q tx ["INSERT INTO foo(xt$id, a) values(42, 42)"])
                              (q conn ["SELECT foo.a FROM foo"]))))
      (is (= [] (q conn ["SELECT foo.a FROM foo"]))))

    (testing "insert it"
      (q conn ["SET TRANSACTION READ WRITE"])
      (jdbc/with-transaction [tx conn]
        (q tx ["INSERT INTO foo(xt$id, a) values(42, 42)"]))
      (testing "read after write"
        (is (= [{:a 42}] (q conn ["SELECT foo.a FROM foo"])))))

    (testing "update it"
      (tx! conn ["UPDATE foo SET a = foo.a + 1 WHERE foo.xt$id = 42"])
      (is (= [{:a 43}] (q conn ["SELECT foo.a FROM foo"]))))

    (testing "delete it"
      (tx! conn ["DELETE FROM foo WHERE foo.xt$id = 42"])
      (is (= [] (q conn ["SELECT foo.a FROM foo"]))))))

(when (psql-available?)
  (deftest psql-dml-test
    (psql-session
     (fn [send read]
       (testing "set transaction"
         (send "SET TRANSACTION READ WRITE;\n")
         (is (str/includes? (read) "SET TRANSACTION")))

       (testing "begin"
         (send "BEGIN;\n")
         (is (str/includes? (read) "BEGIN")))

       (testing "insert"
         (send "INSERT INTO foo (xt$id, a) values (42, 42);\n")
         (is (str/includes? (read) "INSERT 0 0")))

       (testing "insert 2"
         (send "INSERT INTO foo (xt$id, a) values (366, 366);\n")
         (is (str/includes? (read) "INSERT 0 0")))

       (testing "commit"
         (send "COMMIT;\n")
         (is (str/includes? (read) "COMMIT")))

       (testing "read your own writes"
         (send "SELECT foo.a FROM foo;\n")
         (let [s (read)]
           (is (str/includes? s "42"))
           (is (str/includes? s "366"))))

       (testing "delete"
         (send "BEGIN READ WRITE;\n")
         (read)
         (send "DELETE FROM foo;\n")
         (let [s (read)]
           (is (str/includes? s "DELETE 0"))
           (testing "no description sent"
             (is (not (str/includes? s "_iid")))))))))

  (deftest psql-dml-at-prompt-test
    (psql-session
     (fn [send read]
       (send "INSERT INTO foo(xt$id, a) VALUES (42, 42);\n")
       (is (str/includes? (read) "INSERT 0 0"))))))

(deftest dml-param-test
  (with-open [conn (jdbc-conn)]
    (tx! conn ["INSERT INTO foo (xt$id, a) VALUES (?, ?)" 42 "hello, world"])
    (is (= [{:a "hello, world"}] (q conn ["SELECT foo.a FROM foo where foo.xt$id = 42"])))))

;; SQL:2011 p1037 1,a,i
(deftest set-transaction-in-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (q conn ["BEGIN"])
    (is (thrown-with-msg?
          PSQLException
          #"ERROR\: invalid transaction state \-\- active SQL\-transaction"
          (q conn ["SET TRANSACTION READ WRITE"])))))

(deftest begin-in-a-transaction-error-test
  (with-open [conn (jdbc-conn)]
    (q conn ["BEGIN"])
    (is (thrown-with-msg?
          PSQLException
          #"ERROR\: invalid transaction state \-\- active SQL\-transaction"
          (q conn ["BEGIN"])))))

(defn- current-ts [conn]
  (:a (first (q conn ["select current_timestamp a from (values (0)) a (b)"]))))

(deftest half-baked-clock-test
  ;; goal is to test time basis params are sent with queries
  ;; no support yet for SET/SHOW TIME ZONE so testing with internals to
  ;; provoke some clock specialised behaviour and make sure something not totally wrong happens
  (require-server)
  (let [custom-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03Z") (ZoneOffset/ofHoursMinutes 3 12))]

    (swap! (:server-state *server*) assoc :clock custom-clock)

    (testing "server zone is inherited by conn session"
      (with-open [conn (jdbc-conn)]
        (is (= {:clock custom-clock} (session-variables (get-last-conn) [:clock])))
        (is (= "2022-08-16T14:20:03+03:12" (current-ts conn)))))

    (swap! (:server-state *server*) assoc :clock (Clock/systemDefaultZone))

    ;; I am not 100% this is the behaviour we actually want as it stands
    ;; going off repeatable-read steer from ADR-40
    (testing "current ts instant is pinned during a tx, regardless of what happens to the session clock"
      (with-open [conn (jdbc-conn)]
        (let [{:keys [conn-state]} (get-last-conn)]
          (swap! conn-state assoc-in [:session :clock] (Clock/fixed Instant/EPOCH ZoneOffset/UTC)))

        (jdbc/with-transaction [tx conn]
          (is (= "1970-01-01T00:00Z" (current-ts tx)))
          (Thread/sleep 10)
          (is (= "1970-01-01T00:00Z" (current-ts tx)))
          (testing "inside a transaction, changing the zone is permitted, but the instant is fixed, regardless of the session clock"
            (let [{:keys [conn-state]} (get-last-conn)]
              (swap! conn-state assoc-in [:session :clock] custom-clock))
            (is (= "1970-01-01T03:12+03:12" (current-ts tx)))))

        (is (= "2022-08-16T14:20:03+03:12" (current-ts conn)))))))

(deftest set-time-zone-test
  (require-server {:num-threads 2})
  (let [server-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03Z") (ZoneOffset/ofHoursMinutes 3 12))]
    (swap! (:server-state *server*) assoc :clock server-clock)
    (with-open [conn (jdbc-conn)]
      ;; for sanity
      (testing "expect server clock"
        (is (= "2022-08-16T14:20:03+03:12" (current-ts conn))))

      (testing "utc"
        (q conn ["SET TIME ZONE '+00:00'"])
        (is (= "2022-08-16T11:08:03Z" (current-ts conn))))

      (testing "tz is session scoped"
        (with-open [conn2 (jdbc-conn)]
          (is (= "2022-08-16T14:20:03+03:12" (current-ts conn2)))
          (is (= "2022-08-16T11:08:03Z" (current-ts conn)))
          (q conn2 ["SET TIME ZONE '+00:01'"])
          (is (= "2022-08-16T11:09:03+00:01" (current-ts conn2)))
          (is (= "2022-08-16T11:08:03Z" (current-ts conn)))))

      (testing "postive sign"
        (q conn ["SET TIME ZONE '+01:34'"])
        (is (= "2022-08-16T12:42:03+01:34" (current-ts conn))))

      (testing "negative sign"
        (q conn ["SET TIME ZONE '-01:04'"])
        (is (= "2022-08-16T10:04:03-01:04" (current-ts conn))))

      (jdbc/with-transaction [tx conn]
        (testing "in a transaction, inherits session tz"
          (is (= "2022-08-16T10:04:03-01:04" (current-ts tx))))

        (testing "tz can be modified in a transaction"
          (q conn ["SET TIME ZONE '+00:00'"])
          (is (= "2022-08-16T11:08:03Z" (current-ts conn)))))

      (testing "SET is a session operator, so the tz still applied to the session"
        (is (= "2022-08-16T11:08:03Z" (current-ts conn)))))))

(deftest zoned-dt-printing-test
  (require-server)
  (let [custom-clock (Clock/fixed (Instant/parse "2022-08-16T11:08:03Z")
                                  ;; lets hope we do not print zone prefix!
                                  (ZoneId/ofOffset "GMT" (ZoneOffset/ofHoursMinutes 3 12)))]
    (swap! (:server-state *server*) assoc :clock custom-clock)
    (with-open [conn (jdbc-conn)]
      (is (= "2022-08-16T14:20:03+03:12" (current-ts conn))))))

(deftest pg-begin-unsupported-syntax-error-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (is (thrown-with-msg? PSQLException #"ERROR: Invalid SQL query: Parse error at line 1, column 7:" (q conn ["BEGIN not valid sql!"])))
    (is (thrown-with-msg? PSQLException #"ERROR: Invalid SQL query: Parse error at line 1, column 7:" (q conn ["BEGIN SERIALIZABLE"])))))

(deftest begin-with-access-mode-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (testing "DML enabled with BEGIN READ WRITE"
      (q conn ["BEGIN READ WRITE"])
      (q conn ["INSERT INTO foo (xt$id) VALUES (42)"])
      (q conn ["COMMIT"])
      (is (= [{:xt$id 42}] (q conn ["SELECT foo.xt$id from foo"]))))

    (testing "BEGIN access mode overrides SET TRANSACTION"
      (q conn ["SET TRANSACTION READ WRITE"])
      (is (= {:access-mode :read-write} (next-transaction-variables (get-last-conn) [:access-mode])))
      (q conn ["BEGIN READ ONLY"])
      (testing "next-transaction cleared on begin"
        (is (= {} (next-transaction-variables (get-last-conn) [:access-mode]))))
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (q conn ["INSERT INTO foo (xt$id) VALUES (43)"])))
      (q conn ["ROLLBACK"]))))

(deftest start-transaction-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (let [sql #(q conn [%])]

      (sql "START TRANSACTION")
      (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (xt$id) VALUES (42)")))
      (sql "ROLLBACK")

      (sql "START TRANSACTION READ WRITE")
      (sql "INSERT INTO foo (xt$id) VALUES (42)")
      (sql "COMMIT")
      (is (= [{:xt$id 42}] (q conn ["SELECT foo.xt$id from foo"])))

      (testing "access mode overrides SET TRANSACTION"
        (sql "SET TRANSACTION READ WRITE")
        (sql "START TRANSACTION READ ONLY")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (xt$id) VALUES (42)")))
        (sql "ROLLBACK"))

      (testing "set transaction cleared"
        (sql "START TRANSACTION")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (xt$id) VALUES (42)")))
        (sql "ROLLBACK")))))

(deftest set-session-characteristics-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (let [sql #(q conn [%])]
      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      (sql "START TRANSACTION")
      (sql "INSERT INTO foo (xt$id) VALUES (42)")
      (sql "COMMIT")

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      (is (= [{:xt$id 42}] (q conn ["SELECT foo.xt$id from foo"])))

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      (sql "START TRANSACTION")
      (sql "INSERT INTO foo (xt$id) VALUES (43)")
      (sql "COMMIT")

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      (is (= #{{:xt$id 42}, {:xt$id 43}} (set (q conn ["SELECT foo.xt$id from foo"])))))))

(deftest set-app-time-defaults-test
  (with-open [conn (jdbc-conn)]
    (let [sql #(q conn [%])]
      (sql "SET valid_time_defaults TO as_of_now")

      (sql "START TRANSACTION READ WRITE")
      (sql "INSERT INTO foo (xt$id, version) VALUES ('foo', 0)")
      (sql "COMMIT")

      (sql "START TRANSACTION READ WRITE")
      (sql "UPDATE foo SET version = 1 WHERE foo.xt$id = 'foo'")
      (sql "COMMIT")

      (is (= [{:version 1, :xt$valid_from "2020-01-02T00:00Z", :xt$valid_to nil}]
             (q conn ["SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"])))

      (sql "SET valid_time_defaults iso_standard")
      (is (= (set [{:version 0, :xt$valid_from "2020-01-01T00:00Z", :xt$valid_to "2020-01-02T00:00Z"}
                   {:version 1, :xt$valid_from "2020-01-02T00:00Z", :xt$valid_to nil}])
             (set (q conn ["SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"])))))))

;; this demonstrates that session / set variables do not change the next statement
;; its undefined - but we can say what it is _not_.
(deftest implicit-transaction-stop-gap-test
  (with-open [conn (jdbc-conn "autocommit" "false")]
    (let [sql #(q conn [%])]

      (testing "read only"
        (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        (sql "BEGIN")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (xt$id) values (43)")))
        (sql "ROLLBACK")
        (is (= [] (sql "select foo.xt$id from foo"))))

      (sql "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")

      (testing "session access mode inherited"
        (sql "BEGIN")
        (sql "INSERT INTO foo (xt$id) values (42)")
        (sql "COMMIT")
        (testing "despite read write setting read remains available outside tx"
          (is (= [{:xt$id 42}] (sql "select foo.xt$id from foo")))))

      (testing "override session to start a read only transaction"
        (sql "BEGIN READ ONLY")
        (is (thrown-with-msg? PSQLException #"DML is not allowed in a READ ONLY transaction" (sql "INSERT INTO foo (xt$id) values (43)")))
        (sql "ROLLBACK")
        (testing "despite read write setting read remains available outside tx"
          (is (= [{:xt$id 42}] (sql "select foo.xt$id from foo")))))

      (testing "set transaction is not cleared by read/autocommit DML, as they don't start a transaction"
        (sql "SET TRANSACTION READ WRITE")
        (is (= [{:xt$id 42}] (sql "select foo.xt$id from foo")))
        (sql "INSERT INTO foo (xt$id) values (43)")
        (is (= #{{:xt$id 42} {:xt$id 43}} (set (sql "select foo.xt$id from foo"))))
        (is (= {:access-mode :read-write} (next-transaction-variables (get-last-conn) [:access-mode])))))))

(deftest analyzer-error-returned-test
  (testing "Query"
    (with-open [conn (jdbc-conn)]
      (is (thrown-with-msg? PSQLException #"Table variable duplicated: baz" (q conn ["SELECT 1 FROM foo AS baz, baz"])))))
  (testing "DML"
    (with-open [conn (jdbc-conn)]
      (q conn ["BEGIN READ WRITE"])
      (is (thrown-with-msg? PSQLException #"INSERT does not contain mandatory xt\$id column" (q conn ["INSERT INTO foo (a) values (42)"])))
      (is (thrown-with-msg? PSQLException #"INSERT does not contain mandatory xt\$id column" (q conn ["COMMIT"]))))))

(when (psql-available?)
  (deftest psql-analyzer-error-test
    (psql-session
     (fn [send read]
       (send "SELECT 1 FROM foo AS baz, baz;\n")
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (re-find #"Table variable duplicated: baz" s)))

       (send "BEGIN READ WRITE;\n")
       (read)
       ;; no-id
       (send "INSERT INTO foo (x) values (42);\n")
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (re-find #"(?m)INSERT does not contain mandatory xt\$id column" s)))

       (send "COMMIT;\n")
       (let [s (read :err)]
         (is (not= :timeout s))
         (is (re-find #"(?m)INSERT does not contain mandatory xt\$id column" s)))))))

(deftest runtime-error-query-test
  (tu/with-log-level 'xtdb.pgwire :off
    (with-open [conn (jdbc-conn)]
      (is (thrown? PSQLException #"Data error - trim error" (q conn ["SELECT TRIM(LEADING 'abc' FROM a.a) FROM (VALUES ('')) a (a)"]))))))

(deftest runtime-error-commit-test
  (with-open [conn (jdbc-conn)]
    (q conn ["START TRANSACTION READ WRITE"])
    (q conn ["INSERT INTO foo (xt$id) VALUES (TRIM(LEADING 'abc' FROM ''))"])
    #_ ; FIXME #401 - need to show this as an aborted transaction?
    (is (thrown? PSQLException #"Data error - trim error" (q conn ["COMMIT"])))))

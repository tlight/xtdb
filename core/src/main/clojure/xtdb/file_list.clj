(ns xtdb.file-list
  (:require [clojure.set :as clj-set]
            [clojure.string :as string]))

(definterface FileCache
  (addFile [^String file-name] "Add a single file to the cache")
  (addFiles [^clojure.lang.IPersistentCollection #_<String> file-names] "Add a list of files to the cache")
  (removeFile [^String file-name] "Add a single file to the cache")
  (listFiles [] "List all files within the file cache")
  (listFiles [^String prefix] "Last all filenames that start with 'prefix'")
  (init [^Runnable watcher-fn] "Runs a given 'init' function to initially populate the list")
  (watchForChanges [^Runnable watcher-fn] "Start a thread with a function that checks for file changes and updates the cache"))

(defrecord LocalFileCache [!cache]
  FileCache
  (addFile [_ file-name]
    (swap! !cache conj file-name))

  (addFiles [_ file-names]
    (let [file-name-set (set file-names)]
      (swap! !cache clj-set/union file-name-set)))

  (listFiles [this]
    (.listFiles this nil))

  (listFiles [_ prefix]
    (cond->> @!cache
      prefix (filter #(string/starts-with? % prefix))))

  (removeFile [_ file-name]
    (swap! !cache disj file-name))

  (init [this init-fn]
    (init-fn this))

  (watchForChanges [this watcher-fn]
    (watcher-fn this)))

(defn ->local-file-cache [{:keys [init-fn watcher-fn]}]
  (let [cache (->LocalFileCache (atom #{}))]
    (.watchForChanges cache watcher-fn)
    (.init cache init-fn)
    cache))


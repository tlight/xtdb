(ns xtdb.vector.reader-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           (org.apache.arrow.vector.types.pojo FieldType)
           (xtdb.vector IVectorPosition RelationWriter)))

(t/use-fixtures :each tu/with-allocator)

(deftest dynamic-relation-copier-test-different-blocks
  (t/testing "copying rows from different simple col-types from different relations"
    (util/with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                     rel-wtr2 (vw/->rel-writer tu/*allocator*)
                     rel-wtr3 (vw/->rel-writer tu/*allocator*)]
      (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :i64))
            my-colun-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :utf8))]
        (.writeLong my-column-wtr1 42)
        (.writeObject my-colun-wtr2 "forty-two"))
      (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
            copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
        (.copyRow copier1 0)
        (.copyRow copier2 0))
      (t/is (= [{:my-column 42} {:my-column "forty-two"}]
               (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))))

  (t/testing "copying rows from different (composite) col-types from different relations"
    (t/testing "structs"
      (let [combined-field (types/->field "my-column" #xt.arrow/type :union false
                                          (types/->field "struct" #xt.arrow/type :struct false
                                                         (types/col-type->field "toto" [:union #{:null :keyword}])
                                                         (types/col-type->field "bar" [:union #{:null :utf8}])
                                                         (types/->field "foo" #xt.arrow/type :union false
                                                                        (types/col-type->field :i64)
                                                                        (types/col-type->field :f64))))]
        (util/with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                         rel-wtr2 (vw/->rel-writer tu/*allocator*)
                         rel-wtr3 (RelationWriter. tu/*allocator*
                                                   [(vw/->writer (.createVector combined-field tu/*allocator*))])]
          (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :struct))
                my-column-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :struct))]
            (.startStruct my-column-wtr1)
            (-> (.structKeyWriter my-column-wtr1 "foo" (FieldType/notNullable #xt.arrow/type :i64))
                (.writeLong 42))
            (-> (.structKeyWriter my-column-wtr1 "bar" (FieldType/notNullable #xt.arrow/type :utf8))
                (.writeObject "forty-two"))
            (.endStruct my-column-wtr1)
            (.startStruct my-column-wtr2)
            (-> (.structKeyWriter my-column-wtr2 "foo" (FieldType/notNullable #xt.arrow/type :f64))
                (.writeDouble 42.0))
            (-> (.structKeyWriter my-column-wtr2 "toto" (FieldType/notNullable #xt.arrow/type :keyword))
                (.writeObject :my-keyword))
            (.endStruct my-column-wtr2))

          (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
                copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
            (.copyRow copier1 0)
            (.copyRow copier2 0))

          (t/is (= [{:my-column {:foo 42, :bar "forty-two"}}
                    {:my-column {:foo 42.0, :toto :my-keyword}}]
                   (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))
          (t/is (= combined-field
                   (.getField (.colWriter rel-wtr3 "my-column")))))))

    (t/testing "unions"
      (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                  rel-wtr2 (vw/->rel-writer tu/*allocator*)
                  rel-wtr3 (vw/->rel-writer tu/*allocator*)]
        (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :union))
              my-column-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :union))]
          (.writeObject my-column-wtr1 42)
          (.writeObject my-column-wtr1 "forty-two")
          (.writeObject my-column-wtr2 42)
          (.writeObject my-column-wtr2 :my-keyword))

        (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
              copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
          (.copyRow copier1 0)
          (.copyRow copier1 1)
          (.copyRow copier2 0)
          (.copyRow copier2 1))
        (t/is (= [{:my-column 42}
                  {:my-column "forty-two"}
                  {:my-column 42}
                  {:my-column :my-keyword}]
                 (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))
        (t/is (= (types/->field "my-column" #xt.arrow/type :union false
                                (types/col-type->field "i64" :i64)
                                (types/col-type->field "utf8" :utf8)
                                (types/col-type->field "keyword" :keyword))
                 (.getField (.colWriter rel-wtr3 "my-column"))))))

    (t/testing "list"
      (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                  rel-wtr2 (vw/->rel-writer tu/*allocator*)
                  rel-wtr3 (vw/->rel-writer tu/*allocator*)]
        (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :list))
              my-column-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :list))]
          (.startList my-column-wtr1)
          (-> (.listElementWriter my-column-wtr1 (FieldType/notNullable #xt.arrow/type :i64))
              (.writeLong 42))
          (-> (.listElementWriter my-column-wtr1)
              (.writeLong 43))
          (.endList my-column-wtr1)
          (.startList my-column-wtr2)
          (-> (.listElementWriter my-column-wtr2 (FieldType/notNullable #xt.arrow/type :utf8))
              (.writeObject "forty-two"))
          (-> (.listElementWriter my-column-wtr2)
              (.writeObject "forty-three"))
          (.endList my-column-wtr2))

        (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
              copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
          (.copyRow copier1 0)
          (.copyRow copier2 0))

        (t/is (= [{:my-column [42 43]} {:my-column ["forty-two" "forty-three"]}]
                 (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))
        (t/is (= (types/->field "my-column" #xt.arrow/type :union false
                                (types/->field "list" #xt.arrow/type :list false
                                               (types/->field "$data$" #xt.arrow/type :union false
                                                              (types/col-type->field :i64)
                                                              (types/col-type->field :utf8))))
                 (.getField (.colWriter rel-wtr3 "my-column"))))))))

(deftest copying-union-legs-with-different-types-throws
  (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
              rel-wtr2 (vw/->rel-writer tu/*allocator*)
              rel-wtr3 (vw/->rel-writer tu/*allocator*)]
    (-> rel-wtr1
        (.colWriter "my-column" (FieldType/notNullable #xt.arrow/type :union))
        (.legWriter :foo (FieldType/notNullable #xt.arrow/type :i64))
        (.writeLong 42))
    (-> rel-wtr2
        (.colWriter "my-column" (FieldType/notNullable #xt.arrow/type :union))
        (.legWriter :foo (FieldType/notNullable #xt.arrow/type :f64))
        (.writeDouble 42.0))
    (t/is (thrown-with-msg?
           RuntimeException #"Field type mismatch"
           (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
                 copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
             (.copyRow copier1 0)
             (.copyRow copier2 0))))))

(deftest testing-duv->vec-copying
  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              int-vec (.createVector (types/->field "my-int" #xt.arrow/type :i64 false) tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          int-wrt (vw/->writer int-vec)]
      (doto duv-wrt
        (.writeObject 42)
        (.writeObject 43))

      (doto (.rowCopier int-wrt duv)
        (.copyRow 0)
        (.copyRow 1))

      (t/is (= [42 43]
               (tu/vec->vals (vw/vec-wtr->rdr  int-wrt)))
            "duv to monomorphic base type vector copying")))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          list-wrt (vw/->writer list-vec)]
      (.writeObject duv-wrt 42)
      (t/is (thrown-with-msg?
             RuntimeException
             #"illegal copy src vector"
             (.rowCopier list-wrt duv)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          list-wrt (vw/->writer list-vec)
          duv-list-wrt (.legWriter duv-wrt :list (FieldType/notNullable #xt.arrow/type :list))]
      (.startList duv-list-wrt)
      (doto (-> duv-list-wrt
                (.listElementWriter (FieldType/notNullable #xt.arrow/type :i64)))
        (.writeLong 42)
        (.writeLong 43))

      (.endList duv-list-wrt)
      (let [copier (.rowCopier list-wrt duv)]
        (.copyRow copier 0))
      (t/is (= [[42 43]]
               (tu/vec->vals (vw/vec-wtr->rdr  list-wrt)))
            "duv to monomorphic list type vector copying")))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          struct-wrt (vw/->writer struct-vec)]
      (.writeObject duv-wrt 42)
      (t/is (thrown-with-msg?
             RuntimeException
             #"illegal copy src vector"
             (.rowCopier struct-wrt duv)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          struct-wrt (vw/->writer struct-vec)
          duv-struct-wrt (.legWriter duv-wrt :struct (FieldType/notNullable #xt.arrow/type :struct))]
      (.startStruct duv-struct-wrt)
      (-> (.structKeyWriter duv-struct-wrt "foo" (FieldType/notNullable #xt.arrow/type :i64))
          (.writeLong 42))
      (-> (.structKeyWriter duv-struct-wrt "bar" (FieldType/notNullable #xt.arrow/type :utf8))
          (.writeObject "forty-two"))
      (.endStruct duv-struct-wrt)
      (let [copier (.rowCopier struct-wrt duv)]
        (.copyRow copier 0))
      (t/is (= [{:foo 42, :bar "forty-two"}]
               (tu/vec->vals (vw/vec-wtr->rdr struct-wrt)))
            "duv to monomorphic struct type vector copying"))))

(deftest testing-set-writing-reading
  (with-open [set-vec (.createVector (types/->field "my-set" #xt.arrow/type :set false
                                                    (types/col-type->field :i64)) tu/*allocator*)]
    (let [set-wrt (vw/->writer set-vec)]
      (.writeObject set-wrt #{1 2 3})
      (.writeObject set-wrt #{4 5 6})

      (t/is (= [#{1 2 3} #{4 5 6}]
               (tu/vec->vals (vw/vec-wtr->rdr  set-wrt))))

      (let [pos (IVectorPosition/build)]
        (.setPosition pos 0)
        (t/is (= #{1 2 3}
                 (.readObject (.valueReader (vw/vec-wtr->rdr set-wrt) pos)))
              "valueReader testing for set")))))

(deftest struct-normalisation-testing
  (t/testing "structs"
    (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)]
      (let [my-column-wtr1 (.colWriter rel-wtr1 "my_column" (FieldType/notNullable #xt.arrow/type :struct))]
        (.startStruct my-column-wtr1)
        (-> (.structKeyWriter my-column-wtr1 "long_name" (FieldType/notNullable #xt.arrow/type :i64))
            (.writeLong 42))
        (-> (.structKeyWriter my-column-wtr1 "short_name" (FieldType/notNullable #xt.arrow/type :utf8))
            (.writeObject "forty-two"))
        (.endStruct my-column-wtr1)
        (.endRow rel-wtr1))

      (t/is (= [{:my-column {:short-name "forty-two", :long-name 42}}]
               (vr/rel->rows (vw/rel-wtr->rdr rel-wtr1) #xt/key-fn :kebab-case-keyword)))

      (t/is (= [{:my_column {:short_name "forty-two", :long_name 42}}]
               (vr/rel->rows (vw/rel-wtr->rdr rel-wtr1) #xt/key-fn :snake-case-keyword)))

      (t/is (= [{:myColumn {:shortName "forty-two", :longName 42}}]
               (vr/rel->rows (vw/rel-wtr->rdr rel-wtr1) #xt/key-fn :camel-case-keyword))))))

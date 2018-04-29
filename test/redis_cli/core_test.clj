(ns redis-cli.core-test
  (:require [clojure.test :refer :all])
  (:require [redis-cli.core :refer :all])
  (:require [clojure.java.io :as io]))

(deftest a-test
  (testing "get"
    (let [ctx (parse (io/input-stream "resources/test_get.txt") (create-ctx))]
      (is (= (get ctx :num-tokens) 2))
      (is (= (get ctx :command-ty) (symbol "get")))
      (is (= (get ctx :command-keys) '("x")))
      (is (= (get ctx :parse-state) :state-completed))))
  (testing "set"
    (let [ctx (parse (io/input-stream "resources/test_set.txt") (create-ctx))]
      (is (= (get ctx :num-tokens) 3))
      (is (= (get ctx :command-ty (symbol "set"))))
      (is (= (get ctx :command-keys) '("x")))
      (is (= (get ctx :parse-state) :state-completed))))
)

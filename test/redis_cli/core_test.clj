(ns redis-cli.core-test
  (:require [clojure.test :refer :all])
  (:require [redis-cli.core :refer :all])
  (:require [clojure.java.io :as io]))

(deftest a-test
  (testing "basic read"
    (let [ctx (parse (io/input-stream "resources/test_get.txt") (create-ctx))]
      (is (= (get ctx :num-tokens) 2))
      (is (= (get ctx :command-ty) (symbol "get")))
      (is (= (get ctx :command-keys) '("x")))
      (is (= (get ctx :parse-state) :state-completed))))
)

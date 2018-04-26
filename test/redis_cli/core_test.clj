(ns redis-cli.core-test
  (:require [clojure.test :refer :all])
  (:require [redis-cli.core :refer :all])
  (:require [clojure.java.io :as io]))

(deftest a-test
  (testing "basic read"
    (parse (io/input-stream "resources/test_get.txt") (create-ctx))))

(ns shashy.sql.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [shashy.sql.core :as sql]
            [clojure.core :exclude (count)]))

(def connection
  {:classname    "org.h2.Driver"
    :subprotocol "h2:mem:"
    :subname     "sql_test;DB_CLOSE_DELAY=-1" ; keep in memory as long as jvm is open
    :user        "sa"
    :password    ""})

(def query (partial sql/query connection))

;;; Database initialization functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn drop-all-tables
  [db]
  (let [tables ["divisions" "departments" "names" "users"]
        drop-command (fn [t] (format "drop table if exists %s" t))
        drop-all (str/join ";" (map drop-command tables))]
    (jdbc/db-do-commands db drop-all)))

(defn create-all-tables
  [db]
  (jdbc/db-do-commands db
     (jdbc/create-table-ddl :divisions
                            [:id "integer" :primary :key]
                            [:name "varchar (100)" :not :null])
     (jdbc/create-table-ddl :departments
                            [:id "integer" :primary :key]
                            [:name "varchar(100)" :not :null]
                            [:buildings "integer" :not :null]
                            [:division_id "integer" :not :null])
     (jdbc/create-table-ddl :names
                            [:name "varchar(100)" :not :null]
                            [:terminated_at "datetime"]
                            [:rehire "boolean not null default false"])
     (jdbc/create-table-ddl :users
                            [:id "integer" :primary :key]
                            [:name "varchar(100)" :not :null]
                            [:department_id "integer" :not :null])))

(defn seed-all-tables
  [seed-data db]
  (doseq [[table rows] seed-data]
    (let [ins! (partial jdbc/insert! db table)]
      (apply ins! rows))))

(defn setup-database
  [db]
  (do
    (drop-all-tables db)
    (create-all-tables db)
    (seed-all-tables [[:divisions [{:id 1000 :name "Div 1000"}
                                   {:id 2000 :name "Div 2000"}]]
                      [:departments [{:id 100 :name "Dept 100"
                                      :buildings 2 :division_id 1000}
                                     {:id 101 :name "Dept 101"
                                      :buildings 4 :division_id 2000}
                                     {:id 102 :name "Dept 102"
                                      :buildings 8 :division_id 2000}]]
                      [:names [{:name "John"}
                               {:name "Jules"}
                               {:name "John"}
                               {:name "Jim"
                                :terminated_at (java.sql.Timestamp. 1)
                                :rehire true}]]
                      [:users [{:id 1 :name "User 1" :department_id 100}
                               {:id 2 :name "User a2" :department_id 100}
                               {:id 3 :name "User 3" :department_id 101}]]]
                     db)))

(use-fixtures :once (fn [f]
                      (setup-database connection)
                      (f)))

;;; Demonstrating the use of the basic select ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest selecting-fields-on-a-single-table
  ; Note fields are renamed by default from snake_case to kebab-case
  (let [table-data  [{:id 1 :name "User 1" :department-id 100}
                     {:id 2 :name "User a2" :department-id 100}
                     {:id 3 :name "User 3" :department-id 101}]
        query (query :users)]
    (testing "all the fields are returned by default if none are specified"
      (is (= table-data (sql/exec query))))
    (testing "only the fields specified are returned when fields are specified"
      (is (= (map (fn [m] (select-keys m [:name])) table-data)
             (-> query
                 (sql/fields [:name])
                 sql/exec))))
    (testing "fields can be renamed using the keyword syntax"
      (is (= (map (fn [m] {:user-name (:name m)}) table-data)
             (-> query
                 (sql/fields [[:name :user_name]])
                 sql/exec))))
    (testing "fields can be renamed using the string syntax"
      (is (= (map (fn [m] {:user-name (:name m)}) table-data)
             (-> query
                 (sql/fields ["name as user_name"])
                 sql/exec))))
    (testing "database functions can be invoked on fields using the string syntax"
      (is (= (repeat 3 {:short-name "Use"})
             (-> query
                 (sql/fields ["left(name, 3) as short_name"])
                 sql/exec))))))

;;; Demonstrating Limits ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest testing-limiting-rows
  (testing "limit limits the number of rows returned by a query"
    (is (= 2
           (-> :users query (sql/limit 2) sql/exec count)))
    (is (= 1
           (-> :users query (sql/limit 1) sql/exec count)))))

;;; Field renaming ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest field-renaming
  (testing "fields are renamed from snake_case to kebab-case by the default transform"
    (is (= [{:department-id 100} {:department-id 100} {:department-id 101}]
           (-> (query :users)
               (sql/fields [:department_id])
               (sql/exec)))))
  (testing "the default transform can be switched off"
    (is (= [{:department_id 100} {:department_id 100} {:department_id 101}]
           (-> (query :users)
               (sql/fields [:department_id])
               (sql/transform-with identity)
               (sql/exec)))))
  (testing "maps can be transformed according to a supplied transform-fn"
    (is (= [{"Department_id" 100 "Id" 1} {"Department_id" 100 "Id" 2} {"Department_id" 101 "Id" 3}]
           (-> (query :users)
               (sql/fields [:department_id :id])
               (sql/transform-with
                 (fn [m]
                   (zipmap (map (comp str/capitalize name) (keys m))
                           (vals m))))
               sql/exec))))
  (testing "transforms can be chained together"
    (is (= [{"Department_id" 100 "Id" 1} {"Department_id" 100 "Id" 2} {"Department_id" 101 "Id" 3}]
           (-> (query :users)
               (sql/fields [:department_id :id])
               (sql/transform-with
                 (fn [m]
                   (zipmap (map name (keys m)) (vals m)))
                 (fn [m]
                   (zipmap (map str/capitalize (keys m)) (vals m))))
               sql/exec))))
  (testing "transform-with overwrites previous transforms"
    (let [query0 (-> :users query (sql/fields [:department_id]))]
      (is (= [{:department-id 100} {:department-id 100} {:department-id 101}]
             (sql/exec query0)))
      (let [query1 (-> query0
                       (sql/transform-with
                         (fn [m] (zipmap (map name (keys m)) (vals m)))))]
        (is (= [{"department_id" 100} {"department_id" 100} {"department_id" 101}]
               (sql/exec query1))))))
  (testing "add-transforms adds to the existing list of transforms"
    (is (= [{"department-id" 100} {"department-id" 100} {"department-id" 101}]
           (-> (query :users)
               (sql/fields [:department_id])
               (sql/add-transforms
                 (fn [m] (zipmap (map name (keys m)) (vals m))))
               sql/exec)))))

;;; Where conditions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest single-where-conditions
  (testing "single conditions"
    (let [users (-> :users query (sql/fields [:id]))]
      (is (= [{:id 1}]
             (-> users
                 (sql/where (= 1 :id))
                 sql/exec)))
      (is (= [{:id 2} {:id 3}]
             (-> users
                 (sql/where (> :id 1))
                 sql/exec)))
      (is (= [{:id 3}]
             (-> users
                 (sql/where (= :id (inc 2)))
                 sql/exec)))
      (is (= [{:id 1} {:id 2} {:id 3}]
             (-> users
                 (sql/where (in :id (range 1 4)))
                 sql/exec)))))
  (testing "null and not null conditions"
    (let [names (-> :names query (sql/fields [:name]))]
      (is (= #{"John" "Jules"}
             (set
               (map :name
                    (-> names
                        (sql/where (null? :terminated_at))
                        sql/exec)))))
      (is (= #{"Jim"}
             (set
               (map :name
                    (-> names
                        (sql/where (not-null? :terminated_at))
                        sql/exec)))))))
  (testing "true and false conditions"
    (let [names (-> :names query (sql/fields [:name]))]
      (is (= #{}
             (set
               (map :name
                    (-> names
                        (sql/where (true? :rehire)))))))

      ))
  (testing "alternate where syntax"
    (let [users (-> :users query (sql/fields [:id]))]
      (is (= [{:id 1}]
             (-> users
                 (sql/where {:id 1})
                 sql/exec)))
      (is (= [{:id 3}]
             (-> users
                 (sql/where {:department_id 101})
                 sql/exec))))))

;;; Testing multiple Where clauses ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest multiple-where-conditions
  (let [users (-> :users query (sql/fields [:id]))]
    (testing "multiple and clauses"
      (is (= [{:id 2}]
             (-> users
                 (sql/where
                   (and (= :department_id 100)
                        (> :id 1)))
                 sql/exec))))
    (testing "multiple or clauses"
      (is (= [{:id 1} {:id 2} {:id 3}]
             (-> users
                 (sql/where
                   (or (= :department_id 101)
                       (in :id (range 1 3))))
                 sql/exec))))
    (testing "alternate syntax for multiple and clauses"
      (is (= [{:id 2}]
             (-> users
                 (sql/where {:id 2
                             :department_id 100})
                 sql/exec))))))

;;; Testing Distinct ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest queries-with-distinct
  (testing "returns distinct fields"
    (let [rows (-> :names query (sql/fields [:name]) sql/distinct sql/exec)]
      (is (= #{"Jim" "John" "Jules"} (set (map :name rows)))))))

;;; Demonstrating the use of aggregation functions e.g. sum, count, max, min ;;;
(deftest aggregrations
  (testing "will return counts"
    (is (= {:id 3}
           (-> (query :users)
               (sql/fields [(count :id :id)])
               sql/exec1)))
    (is (= {:count-id 3} ; default naming is aggregatefn-field
           (-> (query :users)
               (sql/fields [(count :id)])
               sql/exec1)))
    (is (= {:users 3}
           (-> (query :users)
               (sql/fields [(count :id :users)])
               sql/exec1))))
  (testing "will return grouped counts"
    (is (= [{:departments-id 100 :emp-count 2}
            {:departments-id 101 :emp-count 1}
            {:departments-id 102 :emp-count 0}]
           (-> (query :departments)
               (sql/left-join :users [:id :department_id])
               (sql/fields [:departments.id (count :users.id :emp_count)])
               (sql/group-by [:departments.id])
               sql/exec))))
  (testing "will return sums"
    (is (= [{:divisions-id 1000 :sum-b 2}
            {:divisions-id 2000 :sum-b 12}]
           (-> (query :divisions)
               (sql/join :departments [:id :division_id])
               (sql/fields [:divisions.id (sum :buildings :sum_b)])
               (sql/group-by [:divisions.id])
               (sql/order-by [:sum_b])
               sql/exec))))
  (testing "will select based on a having clause"
    (is (= [{:divisions-id 2000 :sum-departments-buildings 12}]
           (-> (query :divisions)
               (sql/join :departments [:id :division_id])
               (sql/fields [:divisions.id (sum :departments.buildings)])
               (sql/group-by [:divisions.id])
               (sql/having (> (sum :buildings) 8))
               sql/exec)))))

;;; Demonstrating the use of joins ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest joins
  (let [table-data [{:users-id 1 :users-name "User 1" :departments-id 100
                     :departments-name "Dept 100"}
                    {:users-id 2 :users-name "User a2" :departments-id 100
                     :departments-name "Dept 100"}
                    {:users-id 3 :users-name "User 3" :departments-id 101
                     :departments-name "Dept 101"}]
        query (-> (query :departments)
                  (sql/join :users [:id :department_id])
                  (sql/fields [:users.id
                               :users.name
                               :departments.id
                               :departments.name]))]
    (testing "will return all records where satisfying the join"
      (is (= table-data
             (sql/exec query))))
    (testing "will compose with where clauses"
      (is (= (drop-last table-data)
             (-> query (sql/where (= :departments.id 100)) sql/exec))))
    (testing "will compose with limit clauses"
      (is (= (take 1 table-data)
             (-> query
                 (sql/where (= :departments.id 100))
                 (sql/limit 1)
                 sql/exec))))
    (testing "will compose with order-by clause"
      (is (= [(second table-data)]
             (-> query
                 (sql/where (= :departments.id 100))
                 (sql/limit 1)
                 (sql/order-by [:users.name :desc])
                 sql/exec))))))

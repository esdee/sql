(ns shashy.sql.core-test
  (:import (java.util GregorianCalendar TimeZone)
           (java.sql Timestamp))
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [shashy.sql.core :as sql]
            [clojure.core :exclude (count)]))

(def connection
  {:classname    "org.h2.Driver"
    :subprotocol "h2:mem:"
    :subname     "sql_test;DB_CLOSE_DELAY=-1" ; keep in memory as long as jvm is open
    :user        "sa"})

(def query (sql/query connection))

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
                                   {:id 2000 :name "Div 2000"}
                                   {:id 9999 :name "I have no departments"}]]
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
                                :terminated_at (Timestamp. 1)
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
        users-query (sql/table query :users)]
    (testing "all the fields are returned by default if none are specified"
      (is (= "select * from users" (sql/to-query-sql users-query)))
      (is (= table-data (sql/exec users-query))))
    (testing "only the fields specified are returned when fields are specified"
      (let [users-query* (sql/fields users-query [:name])]
        (is (= "select name as name from users"
               (sql/to-query-sql users-query*)))
        (is (= (map (fn [m] (select-keys m [:name])) table-data)
               (sql/exec users-query*)))))
    (testing "fields can be renamed using the keyword syntax"
      (let [users-query* (sql/fields users-query [[:name :user_name]])]
        (is (= "select name as user_name from users"
               (sql/to-query-sql users-query*)))
        (is (= (map (fn [m] {:user-name (:name m)}) table-data)
               (sql/exec users-query*)))))
    (testing "fields can be renamed using the string syntax"
      (let [users-query* (sql/fields users-query ["name as user_name"])]
        (is (= "select name as user_name from users"
               (sql/to-query-sql users-query*)))
        (is (= (map (fn [m] {:user-name (:name m)}) table-data)
               (sql/exec users-query*)))))
    (testing "database functions can be invoked on fields using the string syntax"
      (let [users-query* (sql/fields users-query ["left(name, 3) as short_name"])]
        (is (= "select left(name, 3) as short_name from users"
               (sql/to-query-sql users-query*)))
        (is (= (repeat 3 {:short-name "Use"})
               (sql/exec users-query*)))))))

;;; Exec 1 versus exec ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest testing-exec-and-exec1
  (testing "exec will return a sequence of maps"
    (is (= [{:id 1} {:id 2} {:id 3}]
           (-> (sql/table query :users)
               (sql/fields [:id])
               (sql/order-by [:id])
               sql/exec))))
  (testing "exec may return an empty sequence"
    (is (= []
           (-> (sql/table query :users)
               (sql/where (= :id 1000))
               sql/exec))))
  (testing "exec1 will return a single map"
    (is (= {:id 1}
           (-> (sql/table query :users)
               (sql/fields [:id])
               (sql/order-by [:id])
               sql/exec1))))
  (testing "exec1 may return a nil"
    (is (= nil
           (-> (sql/table query :users)
               (sql/where (= :id 1000))
               sql/exec1)))))

;;; Demonstrating Limits ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest testing-limiting-rows
  (testing "limit limits the number of rows returned by a query"
    (is (= 2
           (-> (sql/table query :users) (sql/limit 2) sql/exec count)))
    (is (= 1
           (-> (sql/table query :users) (sql/limit 1) sql/exec count)))))

;;; Field renaming ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest field-renaming
  (testing "fields are renamed from snake_case to kebab-case by the default transform"
    (is (= [{:department-id 100} {:department-id 100} {:department-id 101}]
           (-> (sql/table query :users)
               (sql/fields [:department_id])
               (sql/exec)))))
  (testing "boolean fields are post-pended with a ?"
    (is (= [{:name "Jim"   :rehire? true}
            {:name "John"  :rehire? false}
            {:name "John"  :rehire? false}
            {:name "Jules" :rehire? false}]
           (-> (sql/table query :names)
               (sql/fields [:name :rehire])
               (sql/order-by [:name :asc])
               sql/exec))))
  (testing "the default transform can be switched off"
    (is (= [{:department_id 100} {:department_id 100} {:department_id 101}]
           (-> (sql/table query :users)
               (sql/fields [:department_id])
               sql/no-transforms
               sql/exec))))
  (testing "maps can be transformed according to a supplied transform-fn"
    (is (= [{"Department_id" 100 "Id" 1} {"Department_id" 100 "Id" 2} {"Department_id" 101 "Id" 3}]
           (-> (sql/table query :users)
               (sql/fields [:department_id :id])
               (sql/transform-with
                 (fn [m]
                   (zipmap (map (comp str/capitalize name) (keys m))
                           (vals m))))
               sql/exec))))
  (testing "transforms can be chained together"
    (is (= [{"Department_id" 100 "Id" 1} {"Department_id" 100 "Id" 2} {"Department_id" 101 "Id" 3}]
           (-> (sql/table query :users)
               (sql/fields [:department_id :id])
               (sql/transform-with
                 (fn [m]
                   (zipmap (map name (keys m)) (vals m)))
                 (fn [m]
                   (zipmap (map str/capitalize (keys m)) (vals m))))
               sql/exec))))
  (testing "transform-with overwrites previous transforms"
    (let [query0 (-> (sql/table query :users) (sql/fields [:department_id]))]
      (is (= [{:department-id 100} {:department-id 100} {:department-id 101}]
             (sql/exec query0)))
      (let [query1 (-> query0
                       (sql/transform-with
                         (fn [m] (zipmap (map name (keys m)) (vals m)))))]
        (is (= [{"department_id" 100} {"department_id" 100} {"department_id" 101}]
               (sql/exec query1))))))
  (testing "add-transforms adds to the existing list of transforms"
    (is (= [{"department-id" 100} {"department-id" 100} {"department-id" 101}]
           (-> (sql/table query :users)
               (sql/fields [:department_id])
               (sql/add-transforms
                 (fn [m] (zipmap (map name (keys m)) (vals m))))
               sql/exec)))))

;;; Where conditions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest single-where-conditions
  (testing "single conditions"
    (let [users-query (-> (sql/table query :users) (sql/fields [:id]))]
      (let [users-query* (-> users-query (sql/where (= 1 :id)))]
        (is (= "select id as id from users where (id=?)"
               (sql/to-query-sql users-query*)))
        (is (= [1] (:where-parameters users-query*)))
        (is (= [{:id 1}]
               (sql/exec users-query*))))
      (let [users-query* (-> users-query (sql/where (> :id 1)))]
        (is (= "select id as id from users where (id>?)"
               (sql/to-query-sql users-query*)))
        (is (= [1] (:where-parameters users-query*)))
        (is (= [{:id 2} {:id 3}]
               (sql/exec users-query*))))
      ; You can use the results of fns as parameters in where clauses
      (let [users-query* (-> users-query (sql/where (= :id (inc 2))))]
        (is (= "select id as id from users where (id=?)"
               (sql/to-query-sql users-query*)))
        (is (= [3] (:where-parameters users-query*)))
        (is (= [{:id 3}]
               (sql/exec users-query*))))
      ; You can use clojure seqs for in conditions in where clauses
      (let [users-query* (-> users-query (sql/where (in :id (range 1 4))))]
        (is (= "select id as id from users where (id in(?,?,?))"
               (sql/to-query-sql users-query*)))
        (is (= ['(1 2 3)] (:where-parameters users-query*)))
        (is (= [{:id 1} {:id 2} {:id 3}]
               (sql/exec users-query*))))))
  (testing "null and not null conditions"
    ; select users.id from users where users.terminated_at is null
    (let [names-query (-> (sql/table query :names) (sql/fields [:name]))]
      (let [names-query* (-> names-query (sql/where (null? :terminated_at)))]
        (is (= "select name as name from names where (terminated_at is null)"
               (sql/to-query-sql names-query*)))
        (is (= #{"John" "Jules"}
               (->> (sql/exec names-query*)
                    (map :name)
                    set))))
      ; select users.id from users where users.terminated_at is not null
      (let [names-query* (-> names-query (sql/where (not-null? :terminated_at)))]
        (is (= "select name as name from names where (terminated_at is not null)"
               (sql/to-query-sql names-query*)))
        (is (= #{"Jim"}
               (->> (sql/exec names-query*)
                    (map :name)
                    set))))))
  (testing "true and false conditions"
    (let [names-query (-> (sql/table query :names) (sql/fields [:name]))]
      (let [names-query* (-> names-query (sql/where (true? :rehire)))]
        (is (= "select name as name from names where (rehire=?)" (sql/to-query-sql names-query*)))
        (is (= [true] (:where-parameters names-query*)))
        (is (= #{"Jim"}
               (->> (sql/exec names-query*)
                    (map :name)
                    set))))
      (let [names-query* (-> names-query (sql/where (false? :rehire)))]
        (is (= "select name as name from names where (rehire=?)" (sql/to-query-sql names-query*)))
        (is (= [false] (:where-parameters names-query*)))
        (is (= #{"John" "Jules"}
               (->> (sql/exec names-query*)
                    (map :name)
                    set))))))
  (testing "alternate where syntax"
    (let [users-query (-> (sql/table query :users) (sql/fields [:id]))]
      (let [users-query* (-> users-query (sql/where {:id 1}))]
        (is (= "select id as id from users where ((id=?))" (sql/to-query-sql users-query*)))
        (is (= ['(1)] (:where-parameters users-query*)))
        (is (= [{:id 1}] (sql/exec users-query*))))
      (let [users-query* (-> users-query (sql/where {:department_id (inc 100)}))]
        (is (= "select id as id from users where ((department_id=?))" (sql/to-query-sql users-query*)))
        (is (= ['(101)] (:where-parameters users-query*)))
        (is (= [{:id 3}] (sql/exec users-query*))))))

;;; Date Time translation ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest handling-times
  (let [expected-date (doto (GregorianCalendar/getInstance
                               (TimeZone/getTimeZone "UTC"))
                        (.setTimeInMillis 1))]
    (testing "database times are converted into utc dates automagically"
      (is (= {:terminated-at expected-date}
             (-> (sql/table query :names)
                 (sql/fields [:terminated_at])
                 (sql/where (not-null? :terminated_at))
                 sql/exec1))))
    (testing "dates can be used as parameters without converting to sql timestamps"
      (is (= {:terminated-at expected-date}
             (-> (sql/table query :names)
                 (sql/fields [:terminated_at])
                 (sql/where {:terminated_at expected-date})
                 sql/exec1))))))

;;; Testing multiple Where clauses ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest multiple-where-conditions
  (let [users-query (-> (sql/table query :users) (sql/fields [:id]))]
    (testing "multiple and clauses"
      (let [users-query* (-> users-query
                             (sql/where (and (= :department_id 100)
                                             (> :id 1))))]
        (is (= "select id as id from users where ((department_id=? and id>?))" (sql/to-query-sql users-query*)))
        (is (= ['(100 1)] (:where-parameters users-query*)))
        (is (= [{:id 2}] (sql/exec users-query*)))))
    (testing "multiple or clauses"
      (let [users-query* (-> users-query
                             (sql/where (or (= :department_id 101)
                                            (in :id (range 1 3)))))]
        (is (= "select id as id from users where ((department_id=? or id in(?,?)))"
               (sql/to-query-sql users-query*)))
        (is (= ['(101 (1 2))] (:where-parameters users-query*)))
        (is (= [{:id 1} {:id 2} {:id 3}]
               (sql/exec users-query*)))))
    (testing "alternate syntax for multiple and clauses"
      (let [users-query* (-> users-query
                             (sql/where {:id 2 :department_id 100}))]
        (is (= "select id as id from users where ((department_id=? and id=?))" (sql/to-query-sql users-query*)))
        (is (= ['(100 2)] (:where-parameters users-query*)))
        (is (= [{:id 2}] (sql/exec users-query*)))))))

;;; Testing Distinct ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest queries-with-distinct
  (testing "returns distinct fields"
    ; select distinct names.name from names
    (let [names-query (-> (sql/table query :names) (sql/fields [:name]) sql/distinct)]
      (is (= "select distinct name as name from names" (sql/to-query-sql names-query)))
      (is (= #{"Jim" "John" "Jules"} (set (map :name (sql/exec names-query))))))))

;;; Demonstrating the use of aggregation functions e.g. sum, count, max, min ;;;
(deftest aggregrations
  (testing "will return counts"
    ; select count(id) as id from users limit 1
    (let [users-query (sql/table query :users)]
      (let [users-query* (-> users-query (sql/fields [(count :id :id)]))]
        (is (= "select count(id) as id from users"
               (sql/to-query-sql users-query*)))
        (is (= {:id 3} (sql/exec1 users-query*))))
      (let [users-query* (-> users-query (sql/fields [(count :id)]))]
        ; default naming is aggregatefn-field
        (is (= "select count(id) as count_id from users" (sql/to-query-sql users-query*)))
        (is (= {:count-id 3} (sql/exec1 users-query*))))))
  (testing "will return grouped counts"
    ; select departments.id, count(users.id) as emp_count
    ;   from departments join users on departments.id = users.department_id
    ;   group by departments.id
    (let [query* (-> (sql/table query :departments)
                     (sql/left-join :users [[:id :department_id]])
                     (sql/fields [:departments.id (count :users.id :emp_count)])
                     (sql/group-by [:departments.id]))]
      (is (= (str "select departments.id as departments_id,"
                  "count(users.id) as emp_count from departments "
                  "left join users on departments.id=users.department_id group by departments.id")
             (sql/to-query-sql query*)))
      (is (= [{:departments-id 100 :emp-count 2}
              {:departments-id 101 :emp-count 1}
              {:departments-id 102 :emp-count 0}]
             (sql/exec query*)))))
  (testing "will return sums"
    (let [query* (-> (sql/table query :divisions)
                     (sql/join :departments [[:id :division_id]])
                     (sql/fields [:divisions.id (sum :buildings :sum_b)])
                     (sql/group-by [:divisions.id])
                     (sql/order-by [:sum_b]))]
      (is (= (str "select divisions.id as divisions_id,sum(buildings) as sum_b "
                  "from divisions join departments on divisions.id=departments.division_id "
                  "group by divisions.id order by sum_b")
             (sql/to-query-sql query*)))
      (is (= [{:divisions-id 1000 :sum-b 2}
              {:divisions-id 2000 :sum-b 12}]
             (sql/exec query*)))))
  (testing "will select based on a having clause"
    (let [query* (-> (sql/table query :divisions)
                     (sql/join :departments [[:id :division_id]])
                     (sql/fields [:divisions.id (sum :departments.buildings)])
                     (sql/group-by [:divisions.id])
                     (sql/having (> (sum :buildings) 8)))]
      (is (= (str "select divisions.id as divisions_id,sum(departments.buildings) as sum_departments_buildings "
                 "from divisions join departments on divisions.id=departments.division_id "
                 "group by divisions.id having (sum(buildings)>?)")
             (sql/to-query-sql query*)))
      (is (= [8] (:having-parameters query*)))
      (is (= [{:divisions-id 2000 :sum-departments-buildings 12}]
             (sql/exec query*)))))))

;;; Demonstrating the use of joins ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest joins
  (testing "field names will be qualified with table names automatically"
    (let [query0 (-> (sql/table query :departments)
                    (sql/join :users [[:departments.id :users.department_id]]))
          query1 (-> (sql/table query :departments)
                    (sql/join :users [[:id :department_id]]))]
      (is (= (sql/to-query-sql query0)
             (sql/to-query-sql query1)))))
  (let [table-data [{:users-id 1 :users-name "User 1" :departments-id 100
                     :departments-name "Dept 100"}
                    {:users-id 2 :users-name "User a2" :departments-id 100
                     :departments-name "Dept 100"}
                    {:users-id 3 :users-name "User 3" :departments-id 101
                     :departments-name "Dept 101"}]
        query (-> (sql/table query :departments)
                  (sql/join :users [[:id :department_id]])
                  (sql/fields [:users.id
                               :users.name
                               :departments.id
                               :departments.name]))]
    (testing "will return all records where satisfying the join"
      (is (= (str "select users.id as users_id,users.name as users_name,"
                  "departments.id as departments_id,departments.name as departments_name "
                  "from departments join users on departments.id=users.department_id")
             (sql/to-query-sql query)))
      (is (= table-data
             (sql/exec query))))
    (testing "will compose with where clauses"
      (let [query* (-> query
                       (sql/where (= :departments.id 100)))]
        (is (= (str "select users.id as users_id,users.name as users_name,"
                  "departments.id as departments_id,departments.name as departments_name "
                  "from departments join users on departments.id=users.department_id "
                  "where (departments.id=?)")
               (sql/to-query-sql query*)))
        (is (= [100] (:where-parameters query*)))
        (is (= (drop-last table-data)
               (sql/exec query*)))))
    (testing "will compose with limit clauses"
      ; select users.id as users_id, users.name as users_name, departments.id as departments_id
      ;        departments.name as departments_name
      ;  from departments join users on departments.id = users.department_id
      ;  where departments.id = 100
      ;  limit 1
      (is (= (take 1 table-data)
             (-> query
                 (sql/where (= :departments.id 100))
                 (sql/limit 1)
                 sql/exec))))
    (testing "will compose with order-by clause"
      ; select users.id as users_id, users.name as users_name, departments.id as departments_id
      ;        departments.name as departments_name
      ;  from departments join users on departments.id = users.department_id
      ;  where departments.id = 100
      ;  order by users.name desc
      ;  limit 1
      (is (= [(second table-data)]
             (-> query
                 (sql/where (= :departments.id 100))
                 (sql/limit 1)
                 (sql/order-by [:users.name :desc])
                 sql/exec)))))
  (testing "outer joins"
    (let [expected-data [{:divisions-id 1000 :departments-id 100}
                         {:divisions-id 2000 :departments-id 101}
                         {:divisions-id 2000 :departments-id 102}
                         {:divisions-id 9999 :departments-id nil}]]
      (testing "can use left outer join"
        (let [query* (-> (sql/table query :divisions)
                         (sql/left-join :departments [[:id :division_id]])
                         (sql/fields [:divisions.id :departments.id])
                         (sql/order-by [:divisions.id :asc :departments.id :asc]))]
          (is (= (str "select divisions.id as divisions_id,departments.id as departments_id "
                      "from divisions left join departments on "
                      "divisions.id=departments.division_id "
                      "order by divisions.id asc,departments.id asc")
                 (sql/to-query-sql query*)))
          (is (= expected-data
                 (sql/exec query*)))))
      (testing "can use right outer join"
        (let [query* (-> (sql/table query :departments)
                         (sql/right-join :divisions [[:division_id :id]])
                         (sql/fields [:divisions.id :departments.id])
                         (sql/order-by [:divisions.id :asc :departments.id :asc]))]
          (is (= (str "select divisions.id as divisions_id,departments.id as departments_id "
                      "from departments right join divisions on "
                      "departments.division_id=divisions.id "
                      "order by divisions.id asc,departments.id asc")
                 (sql/to-query-sql query*)))
          (is (= expected-data
                 (sql/exec query*)))))
      (testing "right and left joins are identical if the table order is transpossed"
        (is (= (-> (sql/table query :divisions)
                   (sql/left-join :departments [[:id :division_id]])
                   (sql/fields [:divisions.id :departments.id])
                   (sql/order-by [:divisions.id :asc :departments.id :asc])
                   sql/exec)
               (-> (sql/table query :departments)
                  (sql/right-join :divisions [[:division_id :id]])
                  (sql/fields [:divisions.id :departments.id])
                  (sql/order-by [:divisions.id :asc :departments.id :asc])
                  sql/exec))))))
  (testing "cross joins"
    (let [query (-> (sql/table query :departments)
                    (sql/cross-join :divisions)
                    (sql/fields [:divisions.id :departments.id])
                    (sql/order-by [:divisions.id :asc :departments.id :asc]))]
      (is (= (str "select divisions.id as divisions_id,departments.id as departments_id "
                  "from departments cross join divisions "
                  "order by divisions.id asc,departments.id asc")
             (sql/to-query-sql query)))
      (is (= [{:divisions-id 1000 :departments-id 100}
              {:divisions-id 1000 :departments-id 101}
              {:divisions-id 1000 :departments-id 102}
              {:divisions-id 2000 :departments-id 100}
              {:divisions-id 2000 :departments-id 101}
              {:divisions-id 2000 :departments-id 102}
              {:divisions-id 9999 :departments-id 100}
              {:divisions-id 9999 :departments-id 101}
              {:divisions-id 9999 :departments-id 102}]
             (sql/exec query))))))


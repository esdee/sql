(ns shashy.sql.core
  (:refer-clojure :exclude (group-by set distinct))
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.deprecated :as jdbc-deprecated]
            [shashy.date-utilities.core :as dates])
  (:import [java.sql Connection PreparedStatement ResultSet Timestamp]))

;; --- To Sql Protocol ---------------------------------------------------------
(defprotocol I->Sql
  "Protocol to convert clojure data type to an sql data type for persistence"
  (->sql [clj-type] "Convert a clojure data type to an sql data type"))

(extend-protocol I->Sql
  ; be default we let the JDBC driver handle it
  Object
  (->sql [clj-type] clj-type)

  ; nils are an exception
  nil
  (->sql [_] nil)

  ; date types are another exception
  java.util.Date
  (->sql [dt] (dates/->sql-timestamp dt))

  java.util.GregorianCalendar
  (->sql [calendar] (dates/->sql-timestamp calendar))

  org.joda.time.DateTime
  (->sql [dt] (dates/->sql-timestamp dt)))

;; Default transforms -----------------------------------------------------------
; --- From Sql Protocol --------------------------------------------------------
(defprotocol IFromSql
  "Protocol to convert sql data types to clojure data"
  (from-sql [sql-type] "Convert a sql data type to a clojure data type"))

(extend-protocol IFromSql
  ; by default we just let the JDBC driver handle it
  Object
  (from-sql [sql-type] sql-type)
  ; nil is an exception
  nil
  (from-sql [_] nil)
  ; sql Dates are the exceptions
  java.sql.Date
  (from-sql [sql-date] (dates/->utc sql-date))
  java.sql.Timestamp
  (from-sql [sql-timestamp] (dates/->utc sql-timestamp)))

; --- Retrieval Functions ------------------------------------------------------

; e.g. :id => :id
;      :created_at => :created-at
;      :active => active? if it is a boolean type
(defn- col->key
  [ky vl]
  (let [post (if (= java.lang.Boolean (class vl)) "?")] ; bools get ? added to them
                                                        ; e.g. active => :active?
    (-> ky name (str/replace "_" "-") (str post) keyword)))

; convert a sql map returned from a db to a clojure map
; rename columns and convert data types
(defn- sqlmap->cljmap
  [sql-map]
  (apply merge
         (map (fn [[k v]] {(col->key k v) (from-sql v)})
              sql-map)))

; if no prefixes are supplied, use these instead
(def default-prefixes (atom nil))

(defn set-prefixes!
  [prefixes]
  (reset! default-prefixes prefixes))

(def default-resultset-options
  {:return-keys false
   :result-type :forward-only
   :concurrency :read-only
   :fetch-size 1000})

(defn query
  "Initialize a query object"
  ([]
   {:type ::query
    :table  nil
    :fields []
    :where []
    :having []
    :where-parameters []
    :having-parameters []
    :group-by []
    :order-by []
    :distinct? false
    :joins []
    :limit nil
    :transforms [sqlmap->cljmap]
    :prefixes []
    :connection nil})
  ([connection]
   (assoc (query) :connection connection)))

(defn connection
  [query connection]
  (assoc query :connection connection))

(defn table
  [query table]
  (assoc query :table table))

(defn- uquery
  [query key vals]
  (update-in query [key] concat (flatten (list vals))))

(defn connection
  "Add a connection to a query to be used in executing the query"
  [q c]
  (assoc q :connection c))

(defn limit
  "Impose a limit on the number of rows returned by a query"
  [q l]
  (assoc q :limit l))

(defn- qfname
  "Qualify a field name to table.field"
  [table field]
  (let [field-name (name field)]
    (if (re-seq #"\." field-name)
      field-name ; already qualified, return as is
      (str (name table) "." (name field)))))

(defn ->column-name
  [field-name]
  (str/replace field-name #"\." "_"))

(defn ->column-name-as
  [field]
  (let [field-name (name field)]
    (format "%s as %s" field-name (->column-name field-name))))

(defn format-function-field-seq
  [field-fn field-name rest-values]
  (let [count-vals (count (seq rest-values))
        simple-as (partial format "%s(%s) as %s" field-fn field-name)]
    (cond
     (= 0 count-vals) (simple-as (str field-fn "_" (->column-name field-name)))
     (= 1 count-vals) (simple-as (->column-name (name (last rest-values))))
     :else (format "%s(%s) as %s"
                   field-fn
                   (str/join " " (cons field-name (drop-last rest-values)))
                   (->column-name (name (last rest-values)))))))

(defn parse-field-seq
  "Parse a field argument that is a seq.
   Examples are 
   [:field_x :rename_to_field_y]
   or (left :name 3)
   or (left :name 3 :short_name)"
  [field-or-fn field rest-values]
  (let [field-name (name field)
        column-name (->column-name field-name)
        named-first (name field-or-fn)]
    (if (keyword? field-or-fn)
      (format "%s as %s" named-first column-name)
      (format-function-field-seq named-first field-name rest-values))))

                                        ; (sql/fields [:id (left :name 3)])

(defmacro fields
  [query field-names]
  "Assoc a field name or seq of field names to a query"
  `(->> '~field-names
          (map (fn [field#]
                 (condp = (class field#)
                   clojure.lang.Keyword (->column-name-as field#)
                   String field#
                   (parse-field-seq (first field#)
                                    (second field#)
                                    (drop 2 field#)))))
          (update-in ~query [:fields] concat)))

(defn group-by
  [query group-bys]
  (uquery query :group-by group-bys))

;;; Joins ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- join*
  [{ltable :table :as query} type rtable fields]
  (let [join-syntax (fn [[lf rf]]
                      (str (qfname ltable lf) "="
                           (qfname rtable rf)))
        qualified-joins [(str type " " (name rtable) " on "
                              (str/join " and " (map join-syntax fields)))]]
    (uquery query :joins qualified-joins)))

(defn join
  "Of the form :left-table :right-table [:left_id] [:left_id :right_id]"
  [query rtable fields]
  (join* query "join" rtable fields))

(defn left-join
  "Of the form :left-table :right-table [:left_id] [:left_id :right_id]"
  [query rtable fields]
  (join* query "left join" rtable fields))

(defn right-join
  "Of the form :left-table :right-table [:left_id] [:left_id :right_id]"
  [query rtable fields]
  (join* query "right join" rtable fields))

(defn cross-join
  "Of the form :left-table :right-table"
  [query rtable]
  (uquery query :joins [(str "cross join " (name rtable))]))

(defn order-by
  [query order-bys]
  (let [make-order-by #(str (name %1) " " (when %2 (name %2)))
        qualified-orders (map #(make-order-by (first %) (last %))
                                (partition 2 2 [nil] order-bys))]
    (uquery query :order-by qualified-orders)))

(defn distinct
  "Add distinct clause to query"
  [query]
  (assoc query :distinct? true))

;; Where Functions --------------------------------------------------------------
(defn sql-comparator
  [comparator x y]
  (if (keyword? x) ; (= :id 1)
    [(format "%s%s?" (name x) comparator) y]
    (if (and (string? x)
             (re-seq #"(avg|count|max|min|sum)\(\w+\)" x))
      ; aggregate sql fns
      [(format "%s%s?" x comparator) y]
      (if-let [opposite ({">" "<", "<" ">", ">=" "<=", "<=" ">="} comparator)]
        ; have to flip conditions
        (sql-comparator opposite y x)
        ; just flip the fields
        [(format "%s%s?" (name y) comparator) x]))))

(defmacro sql-and-or
  [comparator forms]
  `(let [sqls# ~forms
        fields# (map first sqls#)
        parameters# (map last sqls#)]
   [(format "(%s)" (str/join ~comparator fields#)) parameters#]))

(defn sql-and
  [& forms]
  (sql-and-or " and " forms))

(defn sql-or
  [& forms]
  (sql-and-or " or " forms))

(defn sql-in
  [x y]
  (let [xk? (keyword? x)
        ?s (str/join "," (repeat (if xk? (count y) (count x)) "?"))]
    [(format "%s in(%s)" (name (if xk? x y)) ?s)
     (if xk? y x)]))

(defn sql-null?
  [n x]
  [(str (name x) " is " n) nil])

(defn sql-aggregate
  [aggregate field]
  (format "%s(%s)" aggregate (name field)))

(def sql-fn-replacements
  {'= #(sql-comparator "=" %1 %2)
   'not= #(sql-comparator "<>" %1 %2)
   '> #(sql-comparator ">" %1 %2)
   '< #(sql-comparator "<" %1 %2)
   '>= #(sql-comparator ">=" %1 %2)
   '<= #(sql-comparator "<=" %1 %2)
   'and sql-and
   'in sql-in
   'not-null? #(sql-null? "not null" %)
   'null? #(sql-null? "null" %)
   'or  sql-or
   'true? #(sql-comparator "=" % true)
   'false? #(sql-comparator "=" % false)
  ;aggregate functions
   'avg #(sql-aggregate "avg" %)
   'count #(sql-aggregate "count" %)
   'max #(sql-aggregate "max" %)
   'min #(sql-aggregate "min" %)
   'sum #(sql-aggregate "sum" %)})

(defn parse-where-or-having
  [form]
  (walk/prewalk-replace sql-fn-replacements form))

(defn- where-or-having-form
  [query form wh whp]
  `(let [[sql# new-parameters#] ~(parse-where-or-having form)]
     (assoc ~query
            ~wh (conj (~wh ~query) sql#)
            ~whp (vec (concat (~whp ~query)
                              (list new-parameters#))))))

(defn- where-or-having-string
  [query s new-parameters wh whp]
  `(assoc ~query
          ~wh (conj (~wh ~query) ~s)
          ~whp (vec (concat (~whp ~query)
                            (list ~@new-parameters)))))

(defn- where-or-having-map
  [query m wh whp]
  (let [form (concat '(and)
                       (reduce (fn [l [k v]]
                                 (conj l (list '= k v)))
                               '()
                               m))]
     (where-or-having-form query form wh whp)))

(defmacro where
  [query & body]
  (let [[f & r] body]
    (if (string? f)
      (where-or-having-string query f r :where :where-parameters)
      (if (map? f)
        (where-or-having-map query f :where :where-parameters)
        (where-or-having-form query f :where :where-parameters)))))

;; Having -----------------------------------------------------------------------
(defmacro having
  [query & body]
  (let [[f r] body]
    (if (string? f)
      (where-or-having-string query f r :having :having-parameters)
      (if (map? f)
        (where-or-having-map query f :having :having-parameters)
        (where-or-having-form query f :having :having-parameters)))))

(defn transform-with
  [query & transforms]
  (assoc query :transforms (vec transforms)))

(defn no-transforms
  [query]
  (transform-with query identity))

(defn add-transforms
  [query & transforms]
  (update-in query
             [:transforms]
             concat
             (flatten (vec transforms))))

(defn- sql-field-names
  [{fields :fields}]
  (if (seq fields) (str/join "," (map name fields))
                   "*"))

(defn- add-clauses
  [s cs]
  (let [clauses (flatten cs)]
    (when (seq clauses)
      (str " " s " "
           (str/join " and " (map #(str "(" % ")") (flatten clauses)))))))

(defn- sql-where
  [{clauses :where}]
  (add-clauses "where" clauses))

(defn- sql-having
  [{clauses :having}]
  (add-clauses "having" clauses))

(defn- sql-group-by
  [{group-bys :group-by}]
  (when (seq group-bys)
    (->> group-bys
         (map name)
         (str/join ",")
         (str " group by "))))

(defn- sql-order-by
  [{order :order-by}]
  (when (seq order)
    (str " order by " (str/join "," order))))

(defn- insert-prefixes
  [prefixes sql]
  (let [add-prefix (fn [s [rgx prefix]]
                     (reduce #(str/replace %1 %2 (str prefix %2))
                             s
                             (clojure.core/set (map first (re-seq rgx s)))))]
    (reduce add-prefix sql prefixes)))

(defn- sql-limit
  [{limit :limit} conn sql]
  (if limit
    (let [db (.toLowerCase (.getDatabaseProductName (.getMetaData conn)))]
      (condp = db
        ; ms sql server
        "microsoft sql server" (str/replace sql "select" (str "select top " limit))
        ; as400
        "db2 udb for as/400" (str sql " fetch first " limit " rows only")
        ; mysql, postgres, h2, sqlite e.t.c
        (str sql " limit " limit)))
    sql))

(defn- sql-distinct
  [{distinct? :distinct?} sql]
  (if distinct?
    (str/replace sql #"select" "select distinct")
    sql))

(defn- to-query-sql*
  ([{:keys [joins prefixes] :as query} conn]
   (let [sql (str "select "
                  (sql-field-names query) " from "
                  (name (:table query)) " "
                  (str/join " " joins) " "
                  (sql-where query)
                  (sql-group-by query)
                  (sql-having query)
                  (sql-order-by query))]
     (->> sql
          (sql-limit query conn)
          (sql-distinct query)
          (insert-prefixes (or prefixes @default-prefixes)))))
  ([{:keys [joins prefixes] :as query}]
   (let [sql (str "select "
                  (sql-field-names query) " from "
                  (name (:table query)) " "
                  (str/join " " joins) " "
                  (sql-where query)
                  (sql-group-by query)
                  (sql-having query)
                  (sql-order-by query))]
     (insert-prefixes (or prefixes @default-prefixes) sql))))

(defn to-query-sql
  [{connection :connection :as query}]
  (-> (to-query-sql* query connection)
      (str/replace #"\s+" " ")
      str/trim))

(defn do-transforms
  [results transforms]
  (if (seq transforms)
      (map (apply comp (reverse transforms)) results)
      results))

(defn- get-results
  [^PreparedStatement stmt sqls parameters queries]
  (let [[_ ms] (dates/ms-taken (.execute stmt))
        #_(put! @log-channel {:type "query" :ms ms :sql sqls :parameters parameters})]
    (->> queries
         (map (fn [{transforms :transforms}]
                (let [results (with-open [^ResultSet rs (.getResultSet stmt)]
                                (do-transforms (doall (jdbc/result-set-seq rs))
                                               transforms))
                      _ (.getMoreResults stmt)]
                  results)))
         (zipmap (map :identifier queries)))))

(defn- prepare-parms
  [queries]
  (->> (flatten (list queries))
       (map #(concat (:set-parameters %)
                     (:where-parameters %)
                     (:having-parameters %)))
       (flatten)
       (remove nil?)
       (mapv ->sql)))

;; Return the results as a map of
;; {:query-identifier [r1 r2 ..] ...}
(defn- execute-statement
  [conn queries]
  (let [sqls (str/join ";" (map #(to-query-sql* % conn) queries))]
    (with-open [^PreparedStatement stmt (apply jdbc/prepare-statement
                                               conn
                                               sqls
                                               default-resultset-options)]
      (let [parms (prepare-parms queries)
            _ (doall (map-indexed #(.setObject stmt (inc %1) %2) parms))]
        (get-results stmt sqls parms queries)))))

(defn multi-query
  "Send n queries to execute using a single statement bound to the same connection.
   The results of the queries will be returned as a map"
  [connection & kqs]
  (let [queries (map #(assoc (last %) :identifier (first %))
                     (partition 2 kqs))] ; [ [k1 q1] [k2 q2] ...])]
  (with-open [conn (jdbc/get-connection connection)]
    (execute-statement conn queries))))

(defn- exec-query
  "Execute a query"
  [{:keys [connection table] :as query}]
  (->> query
       (multi-query connection table)
       (table)))

(defn- exec1-query
  "Return the first result of a query"
  [query]
  (first (exec-query (assoc query :limit 1))))

(defn find-by-id
  "Shorthand find given the table id field (default :id if not supplied) and the id value"
  ([table id-field id-value]
   (-> (query table)
       (where {id-field id-value})
       (exec1-query)))
  ([table id]
   (find-by-id table :id id)))

;; Persisting Records -----------------------------------------------------------
; rename a clojure keyword to a database column name (string)
; e.g. :id => "id"
;      :created-at => "created_at"
;      :active? => "active"
(defn- key->col
  [ky]
  (-> ky name (str/replace "-" "_") (str/replace "?" "")))

; prepare a clojure map for persisting to a rdbms
; keywords become strings, data types are converted
(defn cljmap->sqlmap
  [clj-map]
  (apply merge
         (map (fn [[k v]] {(key->col k) (->sql v)})
              clj-map)))

 ;; Commented out because inserts do not need to be handled in this library
(comment 
 (defn- persist-map
   [{id :id :as record} table]
   (let [db-record (cljmap->sqlmap record)
         [ret ms] (dates/ms-taken
                   (jdbc-deprecated/update-or-insert-values table ["id=?" id] db-record))
         #_(put! @log-channel {:type "insert-or-update" :ms ms :parameters db-record :table table})]
     (or id (:generated_key ret))))


 (defn save-record
   "Persist a clojure map to the database. Uses insert if the :id cannot be matched,
  otherwise uses update. Will return the updated map."
   [table record connection]
   (jdbc-deprecated/with-connection connection (persist-map record table)))

 (defn save-records
   "Persist multiple records."
   [table records connection]
   (jdbc-deprecated/with-connection connection
     (doseq [record records]
       (persist-map record table))))

 (defn insert-record!
   "Insert record into the db. Use this when there is no id field"
   [table record connection]
   (jdbc/insert! connection table (cljmap->sqlmap record)))

 (defn insert-records!
   "Insert record into the db. Use this when there is no id field"
   ([table records]
      (doseq [record records] (insert-record! table record)))
   ([table records conn]
      (doseq [record records] (insert-record! table record conn))))

 (defn do-command
   "Execute a single command against the database"
   [command connection]
   (jdbc-deprecated/with-connection connection
     (jdbc-deprecated/do-commands command))))

(defn do-prepared
   "Execute a single prepared statement against the database"
   [sql parameters connection]
   (jdbc-deprecated/with-connection connection
     (jdbc-deprecated/do-prepared sql (mapv ->sql parameters))))

;; Update table api
(defn update
  "Use this to perform updates"
  [table-name & {:keys [connection]}]
  {:type ::update
   :name table-name
   :set []
   :set-parameters []
   :where []
   :where-parameters []
   :prefixes @default-prefixes
   :connection connection})

(defn sql-set
  [{:keys [set]}]
  (str "set " (str/join "," (flatten set))))

(defn to-update-sql
  [{:keys [prefixes] :as update} conn]
  (let [sql (str "update "
                 (name (:name update)) " "
                 (sql-set update)
                 (sql-where update))]
    sql))

(defn set
  [{:keys [set set-parameters] :as update} parameters-map]
  (let [field-names (map name (keys parameters-map))]
    (assoc update
           :set (conj set (map #(str % "=?") field-names))
           :set-parameters  (conj set-parameters (vals parameters-map)))))

(defn- exec-update
  [{:keys [name connection] :as update}]
  (let [sql (to-update-sql update connection)
        parameters (prepare-parms update)
        [i ms] (dates/ms-taken (do-prepared sql parameters connection))
        #_(put! @log-channel {:type "update" :sql sql :parameters parameters :ms ms})]
    i))

(defn exec
  [{type :type :as query-or-update}]
  (if (= ::query type)
    (exec-query query-or-update)
    (exec-update query-or-update)))

(defn exec1
  [{type :type :as query-or-update}]
  (if (= ::query type)
    (exec1-query query-or-update)
    (exec-update query-or-update)))

(defn finder-map
  [m]
  (let [->field-name #(-> %
                          name
                          (clojure.string/replace #"-" "_")
                          (clojure.string/replace #"/?" "")
                          keyword)
        replacements (->> (keys m)
                          (map #(list % (->field-name %)))
                          flatten
                          (apply hash-map))]
    (clojure.walk/postwalk-replace replacements m)))

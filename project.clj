(defproject sql "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.4"]
                 [joda-time "2.3"]]
  :profiles {:dev {:dependencies [[com.zaxxer/HikariCP "1.4.0"]
                                  [com.impossibl.pgjdbc-ng/pgjdbc-ng "0.3"]
                                  [com.h2database/h2 "1.4.179"]]
                   :plugins []}})

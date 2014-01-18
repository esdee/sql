; Functions for working with LOCALIZED dates and times -------------------------
; Externally everything gets returned as java.util.GregorianCalendar
(ns shashy.date-utilities.core
  (:require [clojure.string :as str]
            [shashy.date-utilities.date-formats :refer (formats)])
  (:import [org.joda.time DateTime DateTimeZone Days Minutes]
           [org.joda.time.format DateTimeFormat DateTimeFormatter ISODateTimeFormat]
           [java.util Date GregorianCalendar]))

; Convert an instance of a joda DateTime to an instance of a GregorianCalendar
(defn- joda->greg-cal
  [joda-time]
  (let [d (GregorianCalendar.)]
    (.setTimeInMillis d (.getMillis joda-time))
    d))

; --- Base Localization functions ----------------------------------------------
(def ^{:doc "The Local Time Zone object"}
  local-tz (DateTimeZone/getDefault))

(defprotocol ILocalize
  "Protocol for handling localization of date-able types"
  (localize [obj] "Localize an object that could be converted to a date-time"))

(extend-protocol ILocalize
  nil
  (localize [_] nil)

  Long
  (localize [long] (joda->greg-cal (DateTime. long)))

  String
  (localize [s]
    (->> formats
         (map #(try (.parseLocalDateTime % s)
                (catch Exception _ nil)))
         (remove nil?)
         (first)
         (.toDateTime)
         (localize)))

  java.sql.Date
  (localize [sql-date] (localize (.getTime sql-date)))

  java.sql.Time
  (localize [sql-time] (localize (.getTime sql-time)))

  java.sql.Timestamp
  (localize [sql-ts] (localize (.getTime sql-ts)))

  java.util.Date
  (localize [date] (localize (.getTime date)))

  java.util.GregorianCalendar
  (localize [calendar] (localize (.getTimeInMillis calendar)))

  org.joda.time.DateTime
  (localize [date-time] (localize (.getMillis date-time))))

(defn- ->joda-time
  [date-able]
  (->> date-able
       (localize)
       (.getTimeInMillis)
       (DateTime.)))

(defn localize-json-date-time
  "Convert a time string of the form yyyy-mm-ddTHH:MM:SS.mmm-dd:dd
   to a localized date-time"
  [json-date]
  (->> json-date
       (.parseLocalDateTime (ISODateTimeFormat/dateTime))
       (.toDateTime)
       (localize)))

(defn ->sql-timestamp
  "Return a date-able object as a java.sql.Time"
  [date-able]
  (java.sql.Timestamp. (.getTimeInMillis (localize date-able))))

(defn local-now []
  "The localized current time"
  (java.util.GregorianCalendar.))

(defn past?
  "Returns true if a date has already occurred."
  [date]
  (let [ms1 (.getTimeInMillis (localize date))
        ms2 (.getTimeInMillis (local-now))]
    (< ms1 ms2)))

(defn- midnight-jt
  [date]
  (let [jt (->joda-time date)]
    (DateTime. (.getYear jt)
                 (.getMonthOfYear jt)
                 (.getDayOfMonth jt)
                 0
                 0)))

(defn midnight
  "Midnight value of date"
  [date]
  (joda->greg-cal (midnight-jt date)))

(defn end-of-day
  "23:59:59 on the day"
  [date]
  (-> (midnight-jt date)
      (.plusMillis (+ (* 23 60 60 1000) (* 59 60 1000) (* 59 1000) 999))
      (joda->greg-cal)))

(defn date-comparator
  [d0 d1]
  (< (.getTimeInMillis (localize d0))
     (.getTimeInMillis (localize d1))))

(defn today?
  "Return true if the date is today"
  [date]
  (= (midnight date) (midnight (local-now))))

; --- Date formatting functions ------------------------------------------------
(defn formatter
  "Create a localized date-time formatter"
  [^String format-string]
  (.withZone (DateTimeFormat/forPattern format-string) local-tz))

(defn ->format
  "Format a date-time able data using the specified formatter"
  [date-time ^DateTimeFormatter date-time-formatter]
  (.print date-time-formatter (->joda-time date-time)))

; --- Payroll date calculations ------------------------------------------------
; Returns a lazy stream of midnights starting with the supplied date and going
; back a day at a time
(defn- midnight-history-stream
  [date]
  (let [mdate (->joda-time (midnight date))]
    (iterate #(.minusDays % 1) mdate)))

; Returns a lazy stream of midnights starting with the supplied date and going
; forward a day at a time
(defn- midnight-future-stream
  [date]
  (let [mdate (->joda-time (midnight date))]
    (iterate #(.plusDays % 1) mdate)))

(defn previous-wednesday
  "Return the previous Wednesday from a date. The Wednesday will be set to midnight.
  A midnight of the passed in date will be sent if the date is a Wednesday.
  The Payroll week starts at Wednesday =>  We Th Fr Sa Su Mo Tu"
  ([date]
   (let [wednesday? #(= 3 (.getDayOfWeek %))]
     (->> (midnight-history-stream date)
          (filter wednesday?)
          (first)
          (localize))))
  ([]
   (previous-wednesday (local-now))))

(defn next-tuesday
  "Return the next Tuesday from a date. The Tuesday will be set to 1 second
   before midnight. A Tuesday will be returned if the date passed in is Tuesday.
   The Payroll week starts as Wednesday => We Th Fr Sa Su Mo Tu"
  [date]
  (let [tuesday? #(= 2 (.getDayOfWeek %))]
    (->> (midnight-future-stream date)
         (filter tuesday?)
         (first)
         (end-of-day))))

(defn days-between
  [date1 date2]
  (.getDays (Days/daysBetween (midnight-jt date1) (midnight-jt date2))))

(defn minutes-between
  [date1 date2]
  (.getMinutes (Minutes/minutesBetween (->joda-time date1)
                                       (->joda-time date2))))


(defn days-range
  "Given 2 dates return an inclusive range of all midnight dates between the
  first and including the last dates.
  Given 1 date use the previous"
  ([date1 date2]
   (let [day1 (midnight-jt date1)
         days (.getDays (Days/daysBetween day1 (midnight-jt date2)))]
     (map #(joda->greg-cal (.plusDays day1 %))
          (range 0 (inc days)))))
  ([day2]
   (days-range (previous-wednesday) day2)))

(defn payroll-weeks
  "Given 2 dates return a seq of payroll weeks.
   Payroll weeks are a map {:starting-at midnight wednesday
                            :ending-at last time on tuesday
                            :days [{:date midnight day0} ... {:date midnight day6}]}.
   The earliest week will start at the previous wednesday of date1
   The last week will end at the next tuesday of date2 or the next tuesday of date1
   whichever is the latest."
  ([date1 date2]
   (let [first-wednesday (previous-wednesday date1)
         tuesday1 (next-tuesday date1)
         tuesday2 (next-tuesday date2)
         last-tuesday (if (< (.getTimeInMillis tuesday2)
                             (.getTimeInMillis tuesday1))
                        tuesday1
                        tuesday2)]
     (map (fn [week-days]
            (hash-map :starting-at (midnight (first week-days))
                      :ending-at (end-of-day (last week-days))
                      :days (map #(hash-map :date (midnight %)) week-days)))
          (partition 7 (days-range first-wednesday last-tuesday)))))
  ([date]
   (payroll-weeks (previous-wednesday) date)))

;; Date comparison and transformation functions --------------------------------
(defn after?
  "Returns true if the date1 is after date2"
  [date1 date2]
  (> (.getTimeInMillis (localize date1))
     (.getTimeInMillis (localize date2))))

(defn equal?
  "Returns true if date1 == date2"
  [date1 date2]
  (= (.getTimeInMillis (localize date1))
     (.getTimeInMillis (localize date2))))

(defn at-least?
  "returns true if date1 is on or after date2"
  [date1 date2]
  (or (after? date1 date2)
      (equal? date1 date2)))

(defn add
  "Return a new date by adding units to a date.
   Supports days, hours, minutes"
  [date unit amount]
  (let [jdt (->joda-time date)]
    (joda->greg-cal
      (condp = unit
        :days (.plusDays jdt amount)
        :hours (.plusHours jdt amount)
        :minutes (.plusMinutes jdt amount)))))

(defn sub
  "Return a new date by subtracting units from a date.
   Supports days, hours"
  [date unit amount]
  (add date unit (- 0 amount)))

(defn check-ending-at
  "If the ending-at is earlier than the starting-at, add 1 day to the ending at"
  [starting-at ending-at]
  (if (after? starting-at ending-at)
    (add ending-at :days 1)
    ending-at))

(defn max-of
  "Given a series of dates, return the latest date"
  [& ds]
  (let [dates (flatten ds)]
    (when (seq dates)
      (if (= 1 (count dates))
        (first dates)
        (reduce #(if (after? %1 %2) %1 %2)
                (flatten dates))))))

;; Smartstaff dates -------------------------------------------------------------i
(defn parse-smartstaff-dates
  "Given a date and a beginning and ending in HHMM format return
   date starting-at and ending-at dates"
  [d s e]
  (try
    (let [date (midnight d)
          ->int #(condp = (count (.trim %))
                   2 (Integer/parseInt %)
                   0 0
                   1 (Integer/parseInt (str (.trim %) "0")))
          ->minutes #(+ (* 60 (->int (apply str (take 2 %))))
                        (->int (apply str (drop 2 %))))
          ->time #(add date :minutes (->minutes (str %)))]
      (if (> (count (.trim (str s))) 0)
        (let [starting-at (->time s)
              e1 (->time e)
              ending-at (if (after? starting-at e1)
                          (add e1 :days 1)
                          e1)]
          [date starting-at ending-at])
        [date nil nil]))
    (catch Exception _
      (println [d s e]))))

;; Timing Macro -----------------------------------------------------------------
(defmacro ms-taken
  "Return a seq where the first item is the resuts of the body and the dates/last item
  is the time taken in ms"
  [& body]
  `(let [start# (. java.lang.System nanoTime)
         results# ~@body
         ms# (/ (double (- (. java.lang.System nanoTime)
                           start#))
                1000000.0)]
     [results# ms#]))

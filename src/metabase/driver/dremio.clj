(ns metabase.driver.dremio
  "Dremio Driver."
  (:require [cheshire.core :as json]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [honey.sql :as sql]
            [java-time :as t]
            [metabase.config.core :as config]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql-jdbc.sync.describe-database :as sync.describe-database]
            [metabase.driver.sql-jdbc.sync.interface :as sync.i]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.legacy-mbql.util :as mbql.u]
            [metabase.query-processor.store :as qp.store]
            [metabase.query-processor.util :as qputil]
            [metabase.util.honey-sql-2 :as h2x]
            [metabase.util.i18n :refer [trs]])
  (:import [java.sql Types]
           [java.time OffsetDateTime OffsetTime ZonedDateTime]))

(driver/register! :dremio, :parent #{:postgres ::legacy/use-legacy-classes-for-read-and-set})

(doseq [[feature supported?] {:table-privileges                false
                              :set-timezone                    false
                              :describe-fields                 false
                              :describe-fks                    false
                              :describe-indexes                false
                              :connection-impersonation        false}]
  (defmethod driver/database-supports? [:dremio feature] [_driver _feature _db] supported?))

(defmethod driver/display-name :dremio [_] "Dremio")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         Configuração da Conexão                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :dremio
  [_ {:keys [user password schema host port ssl]
      :or {user "dbuser", password "dbpassword", schema "", host "localhost", port 31010}
      :as details}]
  (-> {:applicationName    config/mb-app-id-string
       :type :dremio
       :subprotocol "dremio"
       ;; look_in_sources=true garante visibilidade das fontes externas no Dremio
       :subname (str "direct=" host ":" port (if-not (str/blank? schema) (str ";schema=" schema)) ";look_in_sources=true")
       :user user
       :password password
       :host host
       :port port
       :classname "com.dremio.jdbc.Driver"
       :loginTimeout 10
       :ssl (boolean ssl)
       :sendTimeAsDatetime false}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         Descoberta de Metadados                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- build-schema-where-clause [patterns type]
  (let [op (if (= type :exclude) "NOT LIKE" "LIKE")
        conj (if (= type :exclude) " AND " " OR ")]
    (str "(" (str/join conj (repeat (count patterns) (str "TABLE_SCHEMA " op " ?"))) ")")))

;; AJUSTE v1.0.5: Integração com Filtros Nativos do Metabase (Incluir/Excluir) e Campo Schema.
(defmethod driver/describe-database :dremio
  [driver database]
  (let [details (:details database)
        ;; 1. Tenta pegar filtros da caixa de texto separada por vírgula (campo Schema da conexão)
        conn-schema (or (:schema details) "")
        ;; 2. Tenta pegar filtros nativos do Metabase (caixa de Incluir/Excluir do Sync)
        sync-filters (get-in database [:metadata-sync-filters :schemas])
        filter-type (or (:type sync-filters) :include)
        filter-values (or (:values sync-filters) [])
        
        ;; Consolida os padrões: Converte "*" em "%" para o SQL LIKE
        patterns (if (and (str/blank? conn-schema) (empty? filter-values))
                   []
                   (distinct (map #(str/replace (str/trim %) "*" "%") 
                                  (if-not (str/blank? conn-schema) 
                                    (str/split conn-schema #",") 
                                    filter-values))))]
    {:tables
     (set
      (jdbc/query (sql-jdbc.conn/connection-details->spec driver details)
                  (if (empty? patterns)
                    ;; Caso padrão: Busca tudo (exceto sistemas internos)
                    ["SELECT TABLE_SCHEMA AS \"schema\", TABLE_NAME AS \"name\" FROM \"INFORMATION_SCHEMA\".\"TABLES\" WHERE TABLE_SCHEMA NOT IN ('sys', 'information_schema')"]
                    ;; Caso filtrado: Aplica os padrões dinamicamente
                    (into [(str "SELECT TABLE_SCHEMA AS \"schema\", TABLE_NAME AS \"name\" "
                                "FROM \"INFORMATION_SCHEMA\".\"TABLES\" "
                                "WHERE " (build-schema-where-clause patterns filter-type))]
                          patterns))))}))

(defmethod driver/describe-table :dremio
  [& args]
  (apply (get-method driver/describe-table :sql-jdbc) args))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         Restante das Implementações                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/dynamic-database-types-lookup :dremio
  [driver database database-types]
  ((get-method driver/dynamic-database-types-lookup :sql-jdbc) driver database database-types))

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
    [[#"(?i)DOUBLE" :type/Float]
     [#"(?i)INTEGER" :type/Integer]]))

(defmethod sql-jdbc.sync/database-type->base-type :dremio
  [driver column-type]
  (or (database-type->base-type column-type)
      ((get-method sql-jdbc.sync/database-type->base-type :postgres) driver (keyword (str/lower-case (name column-type))))))

(defmethod sql.qp/add-interval-honeysql-form :dremio
  [_ hsql-form amount unit]
  [:timestampadd [:raw (name unit)] amount (h2x/->timestamp hsql-form)])

(defmethod sql.qp/current-datetime-honeysql-form :dremio
  [_driver]
  (h2x/with-database-type-info [:current_timestamp] "timestamp"))

(defn- date-trunc [unit expr] (sql/call :date_trunc (h2x/literal unit) (h2x/->timestamp expr)))

(defmethod sql.qp/date [:dremio :week]
  [_ _ expr]
  (sql.qp/adjust-start-of-week :dremio (partial date-trunc :week) expr))

(defmethod driver/execute-reducible-query :dremio
  [driver {:keys [database settings], {sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [inner-query (-> (assoc inner-query
                          :remark (qputil/query->remark :dremio outer-query)
                          :query  (if (seq params)
                                    (sql.qp/format-honeysql driver (cons sql params))
                                    sql)
                          :max-rows (mbql.u/query->max-rows-limit outer-query))
                        (dissoc :params))
        query       (assoc outer-query :native inner-query)]
    ((get-method driver/execute-reducible-query :postgres) driver query context respond)))

(defmethod sql.qp/format-honeysql :dremio
  [driver honeysql-form]
  (binding [driver/*compile-with-inline-parameters* true]
    ((get-method sql.qp/format-honeysql :postgres) driver honeysql-form)))

(defmethod sql.qp/inline-value [:dremio OffsetDateTime]
  [_ t]
  (format "timestamp '%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)))

(defmethod sql.qp/inline-value [:dremio ZonedDateTime]
  [driver t]
  (sql.qp/inline-value driver (t/offset-date-time t)))

(defmethod driver/db-default-timezone :dremio [_ _]
           "UTC")

(prefer-method
  sql-jdbc.execute/read-column-thunk
  [::legacy/use-legacy-classes-for-read-and-set Types/TIMESTAMP]
  [:postgres Types/TIMESTAMP])

(prefer-method
  sql-jdbc.execute/read-column-thunk
  [::legacy/use-legacy-classes-for-read-and-set Types/TIME]
  [:postgres Types/TIME])
package io.ipolyzos.compute.source.sql;

import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSql {
    public static String CREATE_TRANSACTIONS_TABLE = "CREATE TABLE transactions (\n"
            + "  accountId STRING,\n"
            + "  amount DOUBLE,\n"
            + "  balance DOUBLE,\n"
            + "  customerId STRING,\n"
            + "  ksymbol STRING,\n"
            + "  operation STRING,\n"
            + "  transactionId STRING,\n"
            + "  type STRING,\n"
            + " `eventTime` TIMESTAMP_LTZ(3) METADATA\n,"
            + "  `key` STRING\n,"
            + "     WATERMARK FOR `eventTime` AS `eventTime` - INTERVAL '1' SECOND\n"
            + ") WITH (\n"
            + "  'connector' = 'pulsar',\n"
            + "  'topic' = 'persistent://public/default/transactions-json',\n"
            + "  'key.format' = 'raw',\n"
            + "  'key.fields' = 'key',\n"
            + "  'value.format' = 'json',\n"
            + "  'service-url' = 'pulsar://localhost:6650',\n"
            + "  'admin-url' = 'http://localhost:8080',\n"
            + "  'scan.startup.mode' = 'latest' \n"
            + ")\n";

//    public static String CREATE_TRANSACTIONS_TABLE = "CREATE TABLE transactions (\n"
//            + "  accountId STRING,\n"
//            + "  amount DOUBLE,\n"
//            + "  balance DOUBLE,\n"
//            + "  customerId STRING,\n"
//            + "  kSymbol STRING,\n"
//            + "  operation STRING,\n"
//            + "  transactionId STRING,\n"
//            + "  type STRING,\n"
//            + "  eventTime NUMERIC,\n"
////            + " `time` TIMESTAMP_LTZ(3) METADATA FROM `eventTime`\n,"
//            + "  `key` STRING\n,"
//            + "     WATERMARK FOR eventTime AS eventTime - INTERVAL '1' SECOND\n"
//            + ") WITH (\n"
//            + "  'connector' = 'pulsar',\n"
//            + "  'topic' = 'persistent://public/default/transactions',\n"
//            + "  'format' = 'avro',\n"
//            + "  'service-url' = 'pulsar://localhost:6650',\n"
//            + "  'admin-url' = 'http://localhost:8080',\n"
//            + "  'scan.startup.mode' = 'earliest' \n"
//            + ")\n";
    private static String CREATE_CATALOG = "CREATE CATALOG pulsar\n" +
            "  WITH (\n" +
            "    'type' = 'pulsar-catalog',\n" +
            "    'catalog-admin-url' = 'http://localhost:8080',\n" +
            "    'catalog-service-url' = 'pulsar://localhost:6650'\n" +
            "  )";

    public static void main(String[] args) {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("SHOW DATABASES").print();
        tableEnvironment.executeSql("SHOW CATALOGS").print();
        tableEnvironment.executeSql("SHOW TABLES").print();
        tableEnvironment.executeSql(CREATE_CATALOG).print();
////
        tableEnvironment.executeSql("SHOW CATALOGS").print();
        tableEnvironment.useCatalog("pulsar");

        tableEnvironment.executeSql("SHOW DATABASES").print();

        tableEnvironment.executeSql("CREATE DATABASE IF NOT EXISTS processing");
//
        tableEnvironment.useDatabase("processing");
        tableEnvironment.executeSql("SHOW TABLES");
        tableEnvironment.executeSql(CREATE_TRANSACTIONS_TABLE)
                .print();
        tableEnvironment.executeSql("DESCRIBE `transactions`")
                .print();

//        tableEnvironment.executeSql("SHOW CATALOGS").print();
//
//
////        tableEnvironment.executeSql("SHOW DATABASES").print();
////        tableEnvironment.executeSql("SHOW CATALOGS").print();
////        tableEnvironment.executeSql("SHOW TABLES").print();
        tableEnvironment.executeSql("SELECT * FROM transactions").print();
    }
}

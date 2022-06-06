package io.ipolyzos.config;

import java.util.Optional;

public class AppConfig {
    public static final String SERVICE_HTTP_URL = "http://localhost:8080";
    public static final String SERVICE_URL      = "pulsar://localhost:6650";

    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String CUSTOMERS_TOPIC  = "customers";


    // Input File Sources
    public static final String TRANSACTIONS_FILE_PATH = "/data/transactions.csv";
    public static final String ACCOUNTS_FILE_PATH = "/data/accounts.csv";
    public static final String CUSTOMERS_FILE_PATH = "/data/customers.csv";

    public static final Optional<String> token = Optional.empty();

    public static final String checkpointDir =  "file:///" + System.getProperty("user.dir") +"/checkpoints/";

}

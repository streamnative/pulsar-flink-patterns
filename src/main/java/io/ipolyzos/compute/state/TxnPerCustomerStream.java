package io.ipolyzos.compute.state;

import io.ipolyzos.models.Transaction;
import io.ipolyzos.utils.DataSourceUtils;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TxnPerCustomerStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        DataStream<Transaction> transactionStream = DataSourceUtils
                .getTransactionsStream(environment);

        // Creates a Keyed Stream
        KeyedStream<Transaction, String> customerKeyedStream = transactionStream
                .keyBy(Transaction::getCustomerId);

        // Calculates the number of transactions per Customer Key
        DataStream<String> txnPerCustomerStream = customerKeyedStream
                .process(new TxnPerCustomerFunc())
                .name("TxnPerCustomerFunc")
                .uid("TxnPerCustomerFunc");

        txnPerCustomerStream
                .print()
                .uid("print")
                .name("print");

        environment.execute("Total Transactions Per Customer");
    }
}

package io.ipolyzos.compute.windows;

import io.ipolyzos.compute.windows.functions.TransactionCountFunc;
import io.ipolyzos.models.Transaction;
import io.ipolyzos.utils.DataSourceUtils;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        DataStream<Transaction> transactionStream = DataSourceUtils
                .getTransactionsStream(environment);

        DataStream<String> transactionsWindowedCountStream = transactionStream
                .keyBy(Transaction::getCustomerId)
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new TransactionCountFunc())
                .uid("transactionsWindowedCount")
                .name("transactionsWindowedCount");

        transactionsWindowedCountStream
                .print()
                .uid("print")
                .name("print");

        environment.execute("Transactions Tumbling Window Count");
    }
}

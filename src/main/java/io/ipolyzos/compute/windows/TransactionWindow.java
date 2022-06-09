package io.ipolyzos.compute.windows;

import io.ipolyzos.compute.windows.functions.TransactionCountFunc;
import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Transaction;
import io.ipolyzos.utils.DataSourceUtils;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;

import java.time.Duration;

public class TransactionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        DataStream<Transaction> transactionStream = DataSourceUtils.getTransactionsStream(environment);

        DataStream<String> txnWindowCount =
                transactionStream
                        .keyBy(Transaction::getCustomerId)
                        .window(TumblingEventTimeWindows.of(Time.days(7)))
                        .apply(new TransactionCountFunc())
                        .uid("windowAllCount")
                        .name("windowAllCount");

        txnWindowCount
                .print()
                .uid("print")
                .name("print");

        environment.execute("Transactions Source Stream");
    }

}

package io.ipolyzos.compute.source;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Transaction;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import java.time.Duration;

public class TransactionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);

        PulsarSource<Transaction> transactionSource = PulsarSource.builder()
                .setServiceUrl(AppConfig.SERVICE_URL)
                .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                .setStartCursor(StartCursor.latest())
                .setTopics(AppConfig.TRANSACTIONS_TOPIC)
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Transaction.class), Transaction.class))
                .setSubscriptionName("txn-subs")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();

        WatermarkStrategy<Transaction> watermarkStrategy =
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Transaction>) (txn, l) -> txn.getEventTime()
                        );

        DataStream<Transaction> transactionStream =
                environment
                        .fromSource(transactionSource, watermarkStrategy, "Transactions Source")
                        .name("TransactionSource")
                        .uid("TransactionSource");

        transactionStream
                .print()
                .uid("print")
                .name("print");

        environment.execute("Transactions Source Stream");
    }
}

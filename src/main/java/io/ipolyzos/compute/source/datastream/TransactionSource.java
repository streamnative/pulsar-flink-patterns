package io.ipolyzos.compute.source.datastream;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Transaction;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import java.time.Duration;

/**
 * See also {@link https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/pulsar/}
 * */
public class TransactionSource {
    public static void main(String[] args) throws Exception {
        // 1. Create an execution environment
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        // 2. Create a Pulsar Source
        PulsarSource<Transaction> transactionSource =
                PulsarSource
                        .builder()
                        .setServiceUrl(AppConfig.SERVICE_URL)
                        .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                        .setStartCursor(StartCursor.earliest())
                        .setTopics(AppConfig.TRANSACTIONS_TOPIC_AVRO)
                        .setDeserializationSchema(
                                PulsarDeserializationSchema.pulsarSchema(
                                        AvroSchema.of(Transaction.class), Transaction.class)
                        )
                        .setSubscriptionName("txn-subs")
                        .setUnboundedStopCursor(StopCursor.never())
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .build();

        // 3. Create a watermark strategy
        // We use the event time from the transaction to keep track of the watermarks
        // and we specify that we allow events to be delayed up to 5 seconds.
        //  i.e for each new incoming event if eventTime < maxTimestamp - maxDelay will be discarded
        WatermarkStrategy<Transaction> watermarkStrategy =
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Transaction>) (txn, l) -> txn.getEventTime()
                        );

        // 4. Create a DataStream
        DataStream<Transaction> transactionStream =
                environment
                        .fromSource(transactionSource, watermarkStrategy, "Transactions Source")
                        .name("TransactionSource")
                        .uid("TransactionSource");

        // 5. Print it to the console
        transactionStream
                .print()
                .uid("print")
                .name("print");

        // 6. Execute the program
        environment.execute("Transactions Source Stream");
    }
}

package io.ipolyzos.compute.buffering;

import io.ipolyzos.compute.buffering.handlers.BufferingHandler;
import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Customer;
import io.ipolyzos.models.EnrichedEvent;
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

public class BufferingStream {
    public static void main(String[] args) throws Exception {
        // 1. Initialize the execution environment
        StreamExecutionEnvironment environment = EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        // 2. Initialize Customer Source
        PulsarSource<Customer> customerSource =
                PulsarSource
                        .builder()
                        .setServiceUrl(AppConfig.SERVICE_URL)
                        .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                        .setStartCursor(StartCursor.earliest())
                        .setTopics(AppConfig.CUSTOMERS_TOPIC)
                        .setDeserializationSchema(
                                PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Customer.class), Customer.class)
                        )
                        .setSubscriptionName("customer-subscription")
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .build();

        // 3. Initialize Transactions Source
        PulsarSource<Transaction> transactionSource =
                PulsarSource
                        .builder()
                        .setServiceUrl(AppConfig.SERVICE_URL)
                        .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                        .setStartCursor(StartCursor.earliest())
                        .setTopics(AppConfig.TRANSACTIONS_TOPIC_AVRO)
                        .setDeserializationSchema(
                                PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Transaction.class), Transaction.class)
                        )
                        .setSubscriptionName("txn-subscription")
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .build();

        WatermarkStrategy<Transaction> watermarkStrategy =
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Transaction>) (txn, l) -> txn.getEventTime()
                        );

        // 4. Initialize Transactions Stream
        DataStream<Transaction> transactionStream =
                environment
                        .fromSource(transactionSource, watermarkStrategy, "Transactions Source")
                        .name("TransactionSource")
                        .uid("TransactionSource");

        // 5. Initialize Customer Stream
        DataStream<Customer> customerStream =
                environment
                        .fromSource(customerSource, WatermarkStrategy.forMonotonousTimestamps(), "Customer Source")
                        .name("CustomerSource")
                        .uid("CustomerSource");

        // 6. Data Enrichment
        DataStream<EnrichedEvent> enrichedStream = transactionStream
                .keyBy(Transaction::getCustomerId)
                .connect(customerStream.keyBy(Customer::getCustomerId))
                .process(new BufferingHandler())
                .uid("CustomerLookup")
                .name("CustomerLookup");

        // 7. Print the results
        enrichedStream
                .print()
                .uid("print")
                .name("print");

        // 8. Execute the program
        environment.execute("Data Enrichment Stream");
    }
}

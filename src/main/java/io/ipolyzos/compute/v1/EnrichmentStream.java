package io.ipolyzos.compute.v1;

import io.ipolyzos.compute.v1.handlers.EnrichmentHandler;
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

public class EnrichmentStream {
    public static void main(String[] args) throws Exception {
        // 1. Initialize the execution environment
        StreamExecutionEnvironment environment = EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        // 2. Initialize Customer Source
        PulsarSource<Customer> customerSource = PulsarSource.builder()
                .setServiceUrl(AppConfig.SERVICE_URL)
                .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                .setStartCursor(StartCursor.earliest())
                .setTopics(AppConfig.CUSTOMERS_TOPIC)
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Customer.class), Customer.class))
                .setSubscriptionName("c-subs")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();

        // 3. Initialize Transactions Source
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

        // 4. Initialize Transactions Stream
        DataStream<Transaction> transactionStream =
                environment
                        .fromSource(transactionSource, watermarkStrategy, "Transactions Source")
                        .map(txn -> {
                            String id = txn.getAccountId();
                            txn.setAccountId(id.replace("A", "C"));
                            return txn;
                        })
                        .name("TransactionSource")
                        .uid("TransactionSource");

        // 5. Initialize Customer Stream
        DataStream<Customer> customerStream =
                environment
                        .fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Customer Source")
                        .name("CustomerSource")
                        .uid("CustomerSource");

        DataStream<EnrichedEvent> enrichedStream = transactionStream
                .keyBy(Transaction::getAccountId)
                .connect(customerStream.keyBy(Customer::getClientId))
                .process(new EnrichmentHandler())
                .uid("CustomerLookup")
                .name("CustomerLookup");

        enrichedStream
                .print()
                .uid("print")
                .name("print");

        environment.execute("Data Enrichment Stream");
    }
}

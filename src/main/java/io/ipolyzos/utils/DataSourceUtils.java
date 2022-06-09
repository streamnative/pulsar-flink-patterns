package io.ipolyzos.utils;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Customer;
import io.ipolyzos.models.Transaction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.stream.Stream;

public class DataSourceUtils {
    public static Stream<String> loadDataFile(String fileName) throws IOException {
        return Files.lines(
                Paths.get(System.getProperty("user.dir") +  fileName)
        ).skip(1);
    }

    public static Transaction toTransaction(String line) {
        String[] tokens = line.split(",");
        return new Transaction(
                tokens[0],
                tokens[1],
                tokens[8],
                Timestamp.valueOf(tokens[7].replace("T", " ")).getTime(),
                tokens[2],
                tokens[3],
                parseDouble(tokens[5]),
                parseDouble(tokens[5]),
                tokens[6]
        );
    }


    public static Customer toCustomer(String line) {
        String[] tokens = line.split(",");
        String fullName = String.format("%s %s %s", tokens[8], tokens[9], tokens[10]);

        return new Customer(
                tokens[0],
                tokens[1],
                tokens[7],
                fullName,
                tokens[11],
                tokens[12],
                tokens[13],
                tokens[14],
                tokens[15],
                tokens[16],
                tokens[17],
                tokens[18],
                tokens[19]
        );
    }

    private static Double parseDouble(String id) {
        return Double.parseDouble(id.replace(".0", ""));
    }

    public static DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment environment) {
        PulsarSource<Transaction> transactionSource = PulsarSource.builder()
                .setServiceUrl(AppConfig.SERVICE_URL)
                .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                .setStartCursor(StartCursor.earliest())
                .setTopics(AppConfig.TRANSACTIONS_TOPIC_AVRO)
                .setDeserializationSchema(
                        PulsarDeserializationSchema
                                .pulsarSchema(AvroSchema.of(Transaction.class), Transaction.class)
                )
                .setSubscriptionName("txn-subscription")
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

        return transactionStream;
    }
}

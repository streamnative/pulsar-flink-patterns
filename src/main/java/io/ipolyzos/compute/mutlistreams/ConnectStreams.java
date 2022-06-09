package io.ipolyzos.compute.mutlistreams;

import io.ipolyzos.compute.mutlistreams.functions.RatioCalcFunc;
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

public class ConnectStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);
        environment.setParallelism(1);

        PulsarSource<Transaction> creditSource =
                PulsarSource
                        .builder()
                        .setServiceUrl(AppConfig.SERVICE_URL)
                        .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                        .setStartCursor(StartCursor.earliest())
                        .setTopics(AppConfig.CREDITS_TOPIC)
                        .setDeserializationSchema(
                                PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Transaction.class), Transaction.class)
                        )
                        .setSubscriptionName("credits-subs")
                        .setUnboundedStopCursor(StopCursor.never())
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .build();

        PulsarSource<Transaction> debitsSource =
                PulsarSource
                        .builder()
                        .setServiceUrl(AppConfig.SERVICE_URL)
                        .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                        .setStartCursor(StartCursor.earliest())
                        .setTopics(AppConfig.DEBITS_TOPIC)
                        .setDeserializationSchema(
                                PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Transaction.class), Transaction.class)
                        )
                        .setSubscriptionName("debits-subs")
                        .setUnboundedStopCursor(StopCursor.never())
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .build();

        WatermarkStrategy<Transaction> watermarkStrategy =
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Transaction>) (txn, l) -> txn.getEventTime()
                        );

        DataStream<Transaction> creditStream =
                environment
                        .fromSource(creditSource, watermarkStrategy, "Credits Source")
                        .name("CreditSource")
                        .uid("CreditSource");

        DataStream<Transaction> debitsStream =
                environment
                        .fromSource(debitsSource, watermarkStrategy, "Debit Source")
                        .name("DebitSource")
                        .uid("DebitSource");

        creditStream
                .connect(debitsStream)
                .process(new RatioCalcFunc())
                .print()
        ;
        environment.execute("Connected Streams");
    }
}

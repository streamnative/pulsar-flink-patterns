package io.ipolyzos.compute.source.datastream;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Customer;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;

public class CustomerSource {
    public static void main(String[] args) throws Exception {
        // 1. Create an execution environment
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(true);

        // 2. Create a Pulsar Source
        PulsarSource<Customer> customerSource =
                PulsarSource
                        .builder()
                        .setServiceUrl(AppConfig.SERVICE_URL)
                        .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                        .setStartCursor(StartCursor.earliest())
                        .setTopics(AppConfig.CUSTOMERS_TOPIC)
                        .setDeserializationSchema(
                                PulsarDeserializationSchema.pulsarSchema
                                        (AvroSchema.of(Customer.class), Customer.class)
                        )
                        .setSubscriptionName("customer-subs")
                        .setUnboundedStopCursor(StopCursor.never())
                        .setSubscriptionType(SubscriptionType.Exclusive)
                        .build();

        // 3. Create a watermark strategy
        WatermarkStrategy<Customer> watermarkStrategy
                = WatermarkStrategy.forMonotonousTimestamps();

        // 4. Create a DataStream
        DataStream<Customer> customerStream =
                environment
                        .fromSource(customerSource, watermarkStrategy, "Customer Source")
                        .name("CustomerSource")
                        .uid("CustomerSource");

        // 5. Print it to the console
        customerStream
                .print()
                .uid("print")
                .name("print");

        // 6. Execute the program
        environment.execute("Customer Source Stream");
    }
}

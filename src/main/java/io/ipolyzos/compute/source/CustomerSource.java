package io.ipolyzos.compute.source;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Customer;
import io.ipolyzos.utils.EnvironmentUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;

public class CustomerSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                EnvironmentUtils.initEnvWithWebUI(false);

        PulsarSource<Customer> customerSource = PulsarSource.builder()
                .setServiceUrl(AppConfig.SERVICE_URL)
                .setAdminUrl(AppConfig.SERVICE_HTTP_URL)
                .setStartCursor(StartCursor.earliest())
                .setTopics(AppConfig.CUSTOMERS_TOPIC)
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(AvroSchema.of(Customer.class), Customer.class))
                .setSubscriptionName("c-subs")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();

        DataStream<Customer> customerStream =
                environment
                        .fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Customer Source")
                        .name("CustomerSource")
                        .uid("CustomerSource");

        customerStream
                .print()
                .uid("print")
                .name("print");

        environment.execute("Customer Source Stream");
    }
}

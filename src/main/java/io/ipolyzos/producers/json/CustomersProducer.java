package io.ipolyzos.producers.json;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Customer;
import io.ipolyzos.utils.ClientUtils;
import io.ipolyzos.utils.DataSourceUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class CustomersProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(CustomersProducer.class);

    public static void main(String[] args) throws IOException {
        Stream<Customer> customers = DataSourceUtils.loadDataFile(AppConfig.CUSTOMERS_FILE_PATH)
                .map(DataSourceUtils::toCustomer);

        logger.info("Creating Pulsar Client ...");

        PulsarClient pulsarClient = ClientUtils.initPulsarClient(AppConfig.token);

        logger.info("Creating User Producer ...");
        Producer<Customer> customerProducer
                = pulsarClient.newProducer(JSONSchema.of(Customer.class))
                .producerName("c-producer")
                .topic(AppConfig.CUSTOMERS_TOPIC)
                .blockIfQueueFull(true)
                .create();

        AtomicInteger counter = new AtomicInteger();
        for (Iterator<Customer> it = customers.iterator(); it.hasNext(); ) {
            Customer customer = it.next();

            customerProducer.newMessage()
                    .key(customer.getCustomerId())
                    .value(customer)
                    .eventTime(System.currentTimeMillis())
                    .sendAsync()
                    .whenComplete(callback(counter));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Sent '{}' customer records.", counter.get());
            logger.info("Closing Resources...");
            try {
                customerProducer.close();
                pulsarClient.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }));
    }

    private static BiConsumer<MessageId, Throwable> callback(AtomicInteger counter) {
        return (id, exception) -> {
            if (exception != null) {
                logger.error("❌ Failed message: {}", exception.getMessage());
            } else {
                logger.info("✅ Acked message {} - Total {}", id, counter.getAndIncrement());
            }
        };
    }
}

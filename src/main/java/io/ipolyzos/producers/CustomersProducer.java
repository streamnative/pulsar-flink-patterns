package io.ipolyzos.producers;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Customer;
import io.ipolyzos.utils.ClientUtils;
import java.io.IOException;
import io.ipolyzos.utils.DataSourceUtils;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomersProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(CustomersProducer.class);

    public static void main(String[] args) throws IOException {
        Stream<Customer> customers = DataSourceUtils.loadDataFile(AppConfig.CUSTOMERS_FILE_PATH)
                .map(DataSourceUtils::toCustomer);

        customers.forEach(System.out::println);
        System.exit(0);
        logger.info("Creating Pulsar Client ...");

        PulsarClient pulsarClient = ClientUtils.initPulsarClient(AppConfig.token);

        logger.info("Creating User Producer ...");
        Producer<Customer> customerProducer
                = pulsarClient.newProducer(AvroSchema.of(Customer.class))
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

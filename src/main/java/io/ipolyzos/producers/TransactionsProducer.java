package io.ipolyzos.producers;

import io.ipolyzos.models.Transaction;
import io.ipolyzos.utils.ClientUtils;
import io.ipolyzos.config.AppConfig;
import io.ipolyzos.utils.DataSourceUtils;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class TransactionsProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(TransactionsProducer.class);

    public static void main(String[] args) throws IOException {
        Stream<Transaction> transactions = DataSourceUtils.loadDataFile(AppConfig.TRANSACTIONS_FILE_PATH)
                .map(DataSourceUtils::toTransaction);

        logger.info("Creating Pulsar Client ...");

        PulsarClient pulsarClient = ClientUtils.initPulsarClient(AppConfig.token);

        logger.info("Creating Transactions Producer ...");
        Producer<Transaction> transactionProducer
                = pulsarClient.newProducer(AvroSchema.of(Transaction.class))
                .producerName("txn-producer")
                .topic(AppConfig.TRANSACTIONS_TOPIC)
                .blockIfQueueFull(true)
                .create();

        AtomicInteger counter = new AtomicInteger();
        for (Iterator<Transaction> it = transactions.iterator(); it.hasNext(); ) {
            Transaction transaction = it.next();

            MessageId id = transactionProducer
                    .newMessage()
                    .value(transaction)
                    .eventTime(System.currentTimeMillis())
                    .send();
            counter.getAndIncrement();
            logger.info("✅ Acked message {} - {}.", id, transaction);
        }

        logger.info("Sent '{}' transaction events.", counter.get());
        logger.info("Closing Resources...");

        try {
            transactionProducer.close();
            pulsarClient.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}

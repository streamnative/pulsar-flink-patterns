package io.ipolyzos.compute.buffering.handlers;

import io.ipolyzos.models.Customer;
import io.ipolyzos.models.EnrichedEvent;
import io.ipolyzos.models.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class BufferingHandler extends CoProcessFunction<Transaction, Customer, EnrichedEvent> {
    private static final Logger logger = LoggerFactory.getLogger(BufferingHandler.class);
    private ValueState<Customer> customerState;
    private ValueState<Transaction> transactionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("{}, initializing state ...", this.getClass().getSimpleName());

        customerState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<Customer>("customerState", Customer.class)
                );

        transactionState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<Transaction>("transactionState", Transaction.class)
                );
    }

    @Override
    public void processElement1(Transaction transaction,
                                CoProcessFunction<Transaction, Customer, EnrichedEvent>.Context context,
                                Collector<EnrichedEvent> collector) throws Exception {
        EnrichedEvent enrichedEvent = new EnrichedEvent();
        enrichedEvent.setTransaction(transaction);

        Customer customer = customerState.value();
        if (customer == null) {
            logger.warn("Failed to find state for customer '{}' - buffering transaction.", transaction.getAccountId());
            transactionState.update(transaction);
        } else {
            enrichedEvent.setCustomer(customer);
        }
        collector.collect(enrichedEvent);
    }

    @Override
    public void processElement2(Customer customer,
                                CoProcessFunction<Transaction, Customer, EnrichedEvent>.Context context,
                                Collector<EnrichedEvent> collector) throws Exception {
        customerState.update(customer);

        // check if there is any transaction record waiting for a user event to arrive
        Transaction transaction = transactionState.value();
        if (transaction != null) {
            logger.info("Found a buffering transaction and sending it downstream.");
            collector.collect(new EnrichedEvent(transaction, customer));

            // if there was a transaction we buffered, clear the state
            transactionState.clear();
        }
    }
}

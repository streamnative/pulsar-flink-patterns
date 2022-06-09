package io.ipolyzos.compute.state;

import io.ipolyzos.models.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TxnPerCustomerFunc extends KeyedProcessFunction<String, Transaction, String> {
    private ValueState<Integer> ctState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ctState = getRuntimeContext()
                .getState(new ValueStateDescriptor<Integer>("ctState", Integer.class));
    }

    @Override
    public void processElement(Transaction transaction,
                               KeyedProcessFunction<String, Transaction, String>.Context ctx,
                               Collector<String> out) throws Exception {
        Integer customerTxnNum = ctState.value();
        if (customerTxnNum == null) {
            customerTxnNum = 0;
        }
        ctState.update(customerTxnNum + 1);
        out.collect(
                String.format(
                        "Transactions for customer '%s' - %s.",
                        transaction.getCustomerId(),
                        customerTxnNum + 1
                )
        );
    }
}

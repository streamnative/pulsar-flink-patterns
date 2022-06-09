package io.ipolyzos.compute.windows.functions;

import io.ipolyzos.models.Transaction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TransactionCountFunc implements WindowFunction<Transaction, String, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Transaction> input, Collector<String> out) throws Exception {
        Timestamp windowStart = new Timestamp(window.getStart());
        Timestamp windowEnd   = new Timestamp(window.getEnd());
        List<Transaction> transactions =
                StreamSupport.stream(input.spliterator(), true)
                        .collect(Collectors.toList());
        String output = String.format(
                "[%s - %s]: Customer '%s' made '%s' transactions.", windowStart, windowEnd, key, transactions.stream().count()
        );
        out.collect(output);
    }
}

package io.ipolyzos.compute.windows;

import io.ipolyzos.models.Transaction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CountByWindowAll implements AllWindowFunction<Transaction, String, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Transaction> iterable, Collector<String> collector) throws Exception {
        Timestamp start = new Timestamp(timeWindow.getStart());
        Timestamp end = new Timestamp(timeWindow.getEnd());
        List<Transaction> transactions =
                StreamSupport.stream(iterable.spliterator(), false)
                        .collect(Collectors.toList());
        collector.collect(String.format("[%s %s]: %s", start, end, transactions.stream().count()));
    }
}

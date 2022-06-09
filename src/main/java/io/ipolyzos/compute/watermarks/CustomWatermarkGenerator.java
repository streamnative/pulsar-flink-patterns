package io.ipolyzos.compute.watermarks;

import io.ipolyzos.models.Transaction;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;


// With every new MAX timestamp, every new incoming element
// with event time < max timestamp - max delay will be discarded
public class CustomWatermarkGenerator implements WatermarkGenerator<Transaction> {
    private Long currentMaxTimestamp;
    private Long maxDelay;

    public CustomWatermarkGenerator(Long maxDelay) {
        this.maxDelay = maxDelay;
    }
    /**
     * This methods gets invoked when the datastream emits a new event
     * */
    // maybe emit a watermark on a particular event
    @Override
    public void onEvent(Transaction transaction, long timestamp, WatermarkOutput watermarkOutput) {
        // emitting a watermark is not mandatory
        currentMaxTimestamp = Math.max(currentMaxTimestamp, transaction.getEventTime());
        watermarkOutput.emitWatermark(new Watermark(transaction.getEventTime())); // every event older than THIS event will be discarded
    }

    // can also call onPeriodicEmit to MAYBE emit watermarks regularly
    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1));
    }
}

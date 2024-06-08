package org.project.bolt.cleaning;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BadTimestampBolt extends BaseRichBolt {
    private OutputCollector collector;
    private LinkedList<Long> timestampWindow;
    private static final int WINDOW_SIZE = 10;
    private static final double INTERVAL_THRESHOLD_RATIO = 2.0; // Example threshold ratio

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.timestampWindow = new LinkedList<>();
    }

    @Override
    public void execute(Tuple input) {
        boolean isRejected = input.getBooleanByField("rejected");
        if (isRejected) {
            // Emit the tuple as-is if it's rejected
            collector.emit(input.getValues());
            collector.ack(input);
            return;
        }

        long timestamp = input.getLongByField("ts");

        // Adjust the timestamp if necessary
        Long adjustedTimestamp = adjustTimestamp(timestamp);
        List<Object> values = new ArrayList<>(input.getValues());
        values.set(0, adjustedTimestamp);

        // Emit cleaned data with adjusted timestamp
        collector.emit(new Values(values.toArray()));
        collector.ack(input);

        // Update the timestamp window
        if (timestampWindow.size() >= WINDOW_SIZE) {
            timestampWindow.removeFirst();
        }
        timestampWindow.addLast(adjustedTimestamp);
    }

    private long adjustTimestamp(long timestamp) {
        // Implement logic to adjust the timestamp if it's bad
        if (isBadTimestamp(timestamp)) {
            // Interpolate timestamp based on window
            return interpolateTimestamp();
        }
        return timestamp; // returning the same timestamp if it's good
    }

    private boolean isBadTimestamp(long timestamp) {
        // Check if the interval is very different from the average interval
        if (timestampWindow.size() < 2) {
            return false; // Not enough data to compare, assume it's good
        }
        long lastTimestamp = timestampWindow.getLast();
        long interval = Math.abs(timestamp - lastTimestamp);
        long averageInterval = calculateAverageInterval();
        return interval > INTERVAL_THRESHOLD_RATIO * averageInterval;
    }

    private long calculateAverageInterval() {
        if (timestampWindow.size() < 2) {
            return 0;
        }
        long totalInterval = 0;
        for (int i = 1; i < timestampWindow.size(); i++) {
            totalInterval += Math.abs(timestampWindow.get(i) - timestampWindow.get(i - 1));
        }
        return totalInterval / (timestampWindow.size() - 1);
    }

    private long interpolateTimestamp() {
        if (timestampWindow.size() < 2) {
            return System.currentTimeMillis(); // Fallback to current time if not enough data
        }
        long sum = 0;
        for (long ts : timestampWindow) {
            sum += ts;
        }
        return sum / timestampWindow.size(); // Simple average of the window
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected", "suspicious"));
    }
}

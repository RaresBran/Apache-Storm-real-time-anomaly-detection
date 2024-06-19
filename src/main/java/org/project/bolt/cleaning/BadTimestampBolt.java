package org.project.bolt.cleaning;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BadTimestampBolt extends BaseWindowedBolt {
    private transient OutputCollector collector;
    private static final double INTERVAL_THRESHOLD_RATIO = 2.0; // Example threshold ratio

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        if (tuples.size() != 3) {
            // Skip if we do not have exactly 3 tuples, which shouldn't happen with proper window configuration
            return;
        }

        Tuple lastTuple = tuples.get(2);
        long lastTimestamp = lastTuple.getLongByField("ts");
        boolean isRejected = lastTuple.getBooleanByField("rejected");

        if (isRejected) {
            // Emit the tuple as-is if it's rejected
            collector.emit(lastTuple.getValues());
        } else {
            long secondTimestamp = tuples.get(1).getLongByField("ts");
            long firstTimestamp = tuples.getFirst().getLongByField("ts");
            long adjustedTimestamp = lastTimestamp;

            if (isBadTimestamp(firstTimestamp, secondTimestamp, lastTimestamp)) {
                // Interpolate the last timestamp as the average of the first two if it's bad
                adjustedTimestamp = secondTimestamp + (secondTimestamp - firstTimestamp);
            }

            List<Object> values = new ArrayList<>(lastTuple.getValues());
            values.set(0, adjustedTimestamp); // Set the first field (timestamp) with the adjusted value

            // Emit cleaned data with adjusted timestamp
            collector.emit(new Values(values.toArray()));
        }

        collector.ack(lastTuple);
    }

    private boolean isBadTimestamp(long firstTimestamp, long secondTimestamp, long lastTimestamp) {
        long firstInterval = Math.abs(secondTimestamp - firstTimestamp);
        long secondInterval = Math.abs(lastTimestamp - secondTimestamp);
        return secondInterval > INTERVAL_THRESHOLD_RATIO * firstInterval;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected", "suspicious"));
    }
}

package org.project.bolt.cleaning;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class BadTimestampBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField("ts");

        // Adjust the timestamp if necessary
        long adjustedTimestamp = adjustTimestamp(timestamp);

        // Emit cleaned data with adjusted timestamp
        collector.emit(new Values(
                adjustedTimestamp,
                input.getStringByField("device"),
                input.getDoubleByField("co"),
                input.getDoubleByField("humidity"),
                input.getBooleanByField("light"),
                input.getDoubleByField("lpg"),
                input.getBooleanByField("motion"),
                input.getDoubleByField("smoke"),
                input.getDoubleByField("temp"),
                input.getBooleanByField("rejected")
        ));
        collector.ack(input);
    }

    private long adjustTimestamp(long timestamp) {
        // Implement logic to adjust the timestamp if it's bad
        return timestamp; // Example: returning the same timestamp
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected"));
    }
}

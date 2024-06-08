package org.project.bolt.cleaning;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataOutlierBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(DataOutlierBolt.class);
    private OutputCollector collector;

    // Realistic bounds for each measurement
    private static final double CO_LOWER_BOUND = 0.0; // CO ppm lower bound
    private static final double CO_UPPER_BOUND = 50.0; // CO ppm upper bound

    private static final double HUMIDITY_LOWER_BOUND = 0.0; // Humidity percentage lower bound
    private static final double HUMIDITY_UPPER_BOUND = 100.0; // Humidity percentage upper bound

    private static final double LPG_LOWER_BOUND = 0.0; // LPG concentration lower bound
    private static final double LPG_UPPER_BOUND = 1000.0; // LPG concentration upper bound

    private static final double SMOKE_LOWER_BOUND = 0.0; // Smoke concentration lower bound
    private static final double SMOKE_UPPER_BOUND = 1000.0; // Smoke concentration upper bound

    private static final double TEMP_LOWER_BOUND = -90.0; // Temperature in Celsius lower bound
    private static final double TEMP_UPPER_BOUND = 85.0; // Temperature in Celsius upper bound

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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

        double co = input.getDoubleByField("co");
        double humidity = input.getDoubleByField("humidity");
        double lpg = input.getDoubleByField("lpg");
        double smoke = input.getDoubleByField("smoke");
        double temp = input.getDoubleByField("temp");

        boolean rejected = co < CO_LOWER_BOUND || co > CO_UPPER_BOUND ||
                humidity < HUMIDITY_LOWER_BOUND || humidity > HUMIDITY_UPPER_BOUND ||
                lpg < LPG_LOWER_BOUND || lpg > LPG_UPPER_BOUND ||
                smoke < SMOKE_LOWER_BOUND || smoke > SMOKE_UPPER_BOUND ||
                temp < TEMP_LOWER_BOUND || temp > TEMP_UPPER_BOUND;

        if (rejected) {
            log.warn("Data outlier detected");
        }

        // Adjust the timestamp if necessary
        List<Object> values = new ArrayList<>(input.getValues());
        values.set(10, rejected);

        // Emit cleaned data with adjusted timestamp
        collector.emit(new Values(values.toArray()));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected", "suspicious"));
    }
}

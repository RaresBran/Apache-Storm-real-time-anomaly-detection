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

    private static final double TEMP_LOWER_BOUND = -40.0; // Temperature in Celsius lower bound
    private static final double TEMP_UPPER_BOUND = 85.0; // Temperature in Celsius upper bound

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long ts = input.getLongByField("ts");
        String device = input.getStringByField("device");

        double co = input.getDoubleByField("co");
        double humidity = input.getDoubleByField("humidity");
        boolean light = input.getBooleanByField("light");
        double lpg = input.getDoubleByField("lpg");
        boolean motion = input.getBooleanByField("motion");
        double smoke = input.getDoubleByField("smoke");
        double temp = input.getDoubleByField("temp");

        // Check bounds for each sensor measurement
        if (co < CO_LOWER_BOUND || co > CO_UPPER_BOUND ||
                humidity < HUMIDITY_LOWER_BOUND || humidity > HUMIDITY_UPPER_BOUND ||
                lpg < LPG_LOWER_BOUND || lpg > LPG_UPPER_BOUND ||
                smoke < SMOKE_LOWER_BOUND || smoke > SMOKE_UPPER_BOUND ||
                temp < TEMP_LOWER_BOUND || temp > TEMP_UPPER_BOUND) {
            // Reject data outliers
            log.warn("Data outlier detected");
            return;
        }

        // Emit cleaned data
        collector.emit(new Values(ts, device, co, humidity, light, lpg, motion, smoke, temp));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp"));
    }
}

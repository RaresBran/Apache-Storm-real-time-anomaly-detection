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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MeanShiftBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(MeanShiftBolt.class);
    private OutputCollector collector;
    private Map<String, LinkedList<Double>> sensorWindows;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.sensorWindows = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        long ts = input.getLongByField("ts");
        String device = input.getStringByField("device");

        Map<String, Double> sensorData = new HashMap<>();
        sensorData.put("co", input.getDoubleByField("co"));
        sensorData.put("humidity", input.getDoubleByField("humidity"));
        sensorData.put("lpg", input.getDoubleByField("lpg"));
        sensorData.put("smoke", input.getDoubleByField("smoke"));
        sensorData.put("temp", input.getDoubleByField("temp"));

        boolean rejected = false;

        Map<String, Double> cleanedData = new HashMap<>();

        for (Map.Entry<String, Double> entry : sensorData.entrySet()) {
            String sensor = entry.getKey();
            double value = entry.getValue();

            // Add value to window
            sensorWindows.putIfAbsent(sensor, new LinkedList<>());
            LinkedList<Double> window = sensorWindows.get(sensor);

            // Example window size
            final int windowSize = 10;
            if (window.size() >= windowSize) {
                window.removeFirst();
            }
            window.addLast(value);

            // Check for mean shift
            double mean = window.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            double currentMean = window.getLast();

            // Example threshold for mean shift detection
            final double threshold = 2.0;
            if (Math.abs(currentMean - mean) > threshold) {
                // Reject both segments in case of mean shift
                log.warn("Mean shift detected");
                rejected = true;
            }

            cleanedData.put(sensor, value);
        }

        // Emit cleaned data
        collector.emit(new Values(
                ts,
                device,
                cleanedData.get("co"),
                cleanedData.get("humidity"),
                input.getBooleanByField("light"),
                cleanedData.get("lpg"),
                input.getBooleanByField("motion"),
                cleanedData.get("smoke"),
                cleanedData.get("temp"),
                rejected
        ));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected"));
    }
}

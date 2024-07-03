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

public class FalseSpikeBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(FalseSpikeBolt.class);
    private OutputCollector collector;
    private Map<String, LinkedList<Double>> sensorWindows;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.sensorWindows = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        boolean rejected = input.getBooleanByField("rejected");
        boolean suspicious = input.getBooleanByField("suspicious");
        if (rejected || suspicious) {
            // Emit the tuple as-is if it's rejected
            collector.emit(input.getValues());
            collector.ack(input);
            return;
        }

        long ts = input.getLongByField("ts");
        String device = input.getStringByField("device");

        Map<String, Double> sensorData = new HashMap<>();
        sensorData.put("co", input.getDoubleByField("co"));
        sensorData.put("humidity", input.getDoubleByField("humidity"));
        sensorData.put("lpg", input.getDoubleByField("lpg"));
        sensorData.put("smoke", input.getDoubleByField("smoke"));
        sensorData.put("temp", input.getDoubleByField("temp"));

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

            // Check for spike
            double mean = window.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            final double threshold = 0.5;
            if (value >= mean + (mean * threshold) || value <= mean - (mean * threshold)) {
                log.warn("Detected false spike in sensor {} on device {}: {}", sensor, device, value);
                window.removeLast();
                suspicious = true; // Mark as suspicious
            }
        }

        // Emit cleaned data
        collector.emit(new Values(
                ts,
                device,
                sensorData.get("co"),
                sensorData.get("humidity"),
                input.getBooleanByField("light"),
                sensorData.get("lpg"),
                input.getBooleanByField("motion"),
                sensorData.get("smoke"),
                sensorData.get("temp"),
                input.getBooleanByField("rejected"),
                suspicious
        ));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected", "suspicious"));
    }
}

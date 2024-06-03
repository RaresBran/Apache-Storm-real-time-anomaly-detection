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

public class DataBlockedBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(DataBlockedBolt.class);
    private OutputCollector collector;
    private Map<String, LinkedList<Double>> sensorValues;
    private Map<String, Integer> counts;
    private static final int WINDOW_SIZE = 10; // Window size for statistical analysis
    private static final double THRESHOLD_SIGMA = 3.0; // 3-sigma threshold

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.sensorValues = new HashMap<>();
        this.counts = new HashMap<>();
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

        Map<String, Double> cleanedData = new HashMap<>();

        for (Map.Entry<String, Double> entry : sensorData.entrySet()) {
            String sensor = entry.getKey();
            double value = entry.getValue();

            // Maintain a sliding window of values for each sensor
            sensorValues.putIfAbsent(sensor, new LinkedList<>());
            LinkedList<Double> values = sensorValues.get(sensor);

            if (values.size() >= WINDOW_SIZE) {
                values.removeFirst();
            }
            values.addLast(value);

            // Calculate mean and standard deviation
            double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            double variance = values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0.0);
            double stdDev = Math.sqrt(variance);

            // Check if the value is within the 3-sigma range
            if (Math.abs(value - mean) <= THRESHOLD_SIGMA * stdDev) {
                counts.put(sensor, counts.getOrDefault(sensor, 0) + 1);
            } else {
                counts.put(sensor, 0);
            }

            // Determine appropriate threshold based on sensor
            int threshold;
            switch (sensor) {
                case "temp":
                    threshold = 12; // Longer threshold for temperature
                    break;
                default:
                    threshold = 5; // Default threshold for other sensors
            }

            if (counts.get(sensor) >= threshold) {
                // Data is blocked at constant value, reject
                log.warn("Data blocked for sensor {} on device {} at timestamp {}", sensor, device, ts);
                return;
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
                cleanedData.get("temp")
        ));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp"));
    }
}

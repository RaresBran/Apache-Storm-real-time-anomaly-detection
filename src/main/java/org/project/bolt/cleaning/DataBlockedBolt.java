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

    // Map to store the last seen value and the timestamp when it was first observed
    private static class SensorData {
        double value;
        long startTime;
        LinkedList<Double> window;
    }

    private Map<String, Map<String, SensorData>> deviceSensorData;
    private static final int WINDOW_SIZE = 10; // Window size for statistical analysis
    private static final long BLOCK_DURATION_MS = 12 * 60 * 60 * 1000L; // 12 hours in milliseconds

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.deviceSensorData = new HashMap<>();
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

        for (Map.Entry<String, Double> entry : sensorData.entrySet()) {
            String sensor = entry.getKey();
            double value = entry.getValue();

            // Initialize device data map if it doesn't exist
            deviceSensorData.putIfAbsent(device, new HashMap<>());
            Map<String, SensorData> sensorMap = deviceSensorData.get(device);

            // Initialize sensor data if it doesn't exist
            sensorMap.putIfAbsent(sensor, new SensorData());
            SensorData data = sensorMap.get(sensor);

            if (data.value == value) {
                // Value is the same, check duration
                if (ts - data.startTime >= BLOCK_DURATION_MS) {
                    // Data has been blocked for 12 hours, reject
                    log.warn("Data blocked for sensor {} on device {} at timestamp {}", sensor, device, ts);
                    rejected = true;
                }
            } else {
                // Value has changed, reset start time
                data.value = value;
                data.startTime = ts;
            }

            // Maintain a sliding window of values for each sensor
            data.window = data.window == null ? new LinkedList<>() : data.window;
            if (data.window.size() >= WINDOW_SIZE) {
                data.window.removeFirst();
            }
            data.window.addLast(value);

            // Calculate mean and standard deviation
            double mean = data.window.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            double variance = data.window.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0.0);
            double stdDev = Math.sqrt(variance);

            // Check if the value is within the 3-sigma range
            if (Math.abs(value - mean) > 3.0 * stdDev) {
                // Reset start time if value is outside the 3-sigma range
                data.startTime = ts;
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
                rejected
        ));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected"));
    }
}

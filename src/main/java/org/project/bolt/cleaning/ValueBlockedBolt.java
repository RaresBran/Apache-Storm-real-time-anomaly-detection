package org.project.bolt.cleaning;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ValueBlockedBolt extends BaseRichBolt {
    private transient OutputCollector collector;
    private final long blockDurationMs;

    // Map to store the last seen value and the timestamp when it was first observed
    private static class SensorData implements Serializable {
        double value;
        long startTime;
        LinkedList<Double> window;
    }

    private Map<String, Map<String, SensorData>> deviceSensorData;
    private static final int WINDOW_SIZE = 10; // Window size for statistical analysis

    public ValueBlockedBolt(long blockDurationMs) {
        this.blockDurationMs = blockDurationMs;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.deviceSensorData = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        if (handleRejectedTuple(input))
            return;

        long ts = input.getLongByField("ts");
        String device = input.getStringByField("device");
        boolean suspicious = input.getBooleanByField("suspicious");

        Map<String, Double> sensorData = extractSensorData(input);
        if (!suspicious) {
            suspicious = processSensorData(ts, device, sensorData);
        }

        emitData(input, ts, device, sensorData, suspicious);
        collector.ack(input);
    }

    private boolean handleRejectedTuple(Tuple input) {
        boolean isRejected = input.getBooleanByField("rejected");
        if (isRejected) {
            collector.emit(input.getValues());
            collector.ack(input);
            return true;
        }
        return false;
    }

    private Map<String, Double> extractSensorData(Tuple input) {
        Map<String, Double> sensorData = new HashMap<>();
        sensorData.put("co", input.getDoubleByField("co"));
        sensorData.put("humidity", input.getDoubleByField("humidity"));
        sensorData.put("lpg", input.getDoubleByField("lpg"));
        sensorData.put("smoke", input.getDoubleByField("smoke"));
        sensorData.put("temp", input.getDoubleByField("temp"));
        return sensorData;
    }

    private boolean processSensorData(long ts, String device, Map<String, Double> sensorData) {
        boolean suspicious = false;

        for (Map.Entry<String, Double> entry : sensorData.entrySet()) {
            String sensor = entry.getKey();
            double value = entry.getValue();

            Map<String, SensorData> sensorMap = deviceSensorData.computeIfAbsent(device, k -> new HashMap<>());
            SensorData data = sensorMap.computeIfAbsent(sensor, k -> new SensorData());

            if (isDataBlocked(ts, data, value)) {
                suspicious = true;
            } else {
                resetSensorData(data, value, ts);
            }

            maintainSlidingWindow(data, value);
        }

        return suspicious;
    }

    private boolean isDataBlocked(long ts, SensorData data, double value) {
        return data.value == value && (ts - data.startTime >= blockDurationMs);
    }

    private void resetSensorData(SensorData data, double value, long ts) {
        data.value = value;
        data.startTime = ts;
    }

    private void maintainSlidingWindow(SensorData data, double value) {
        if (data.window == null) {
            data.window = new LinkedList<>();
        }
        if (data.window.size() >= WINDOW_SIZE) {
            data.window.removeFirst();
        }
        data.window.addLast(value);
    }

    private void emitData(Tuple input, long ts, String device, Map<String, Double> sensorData, boolean suspicious) {
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
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected", "suspicious"));
    }
}

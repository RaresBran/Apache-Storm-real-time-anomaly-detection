package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.project.bolt.utility.Thresholds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ThresholdBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ThresholdBolt.class);
    private transient OutputCollector outputCollector;
    private transient Thresholds thresholds;

    private static class SensorWindow implements Serializable {
        boolean thresholdExceeded;
        long anomalyStart;
    }

    private Map<String, SensorWindow> sensorWindows;
    private final Map<String, Double> thresholdMap;

    public ThresholdBolt(Map<String, Double> thresholdMap) {
        this.thresholdMap = thresholdMap;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        thresholds = new Thresholds(
                thresholdMap.get("coLow"), thresholdMap.get("coHigh"),
                thresholdMap.get("humidityLow"), thresholdMap.get("humidityHigh"),
                thresholdMap.get("lpgLow"), thresholdMap.get("lpgHigh"),
                thresholdMap.get("smokeLow"), thresholdMap.get("smokeHigh"),
                thresholdMap.get("tempLow"), thresholdMap.get("tempHigh")
        );

        sensorWindows = new HashMap<>();
        sensorWindows.put("co", new SensorWindow());
        sensorWindows.put("humidity", new SensorWindow());
        sensorWindows.put("light", new SensorWindow());
        sensorWindows.put("lpg", new SensorWindow());
        sensorWindows.put("smoke", new SensorWindow());
        sensorWindows.put("temp", new SensorWindow());
    }

    @Override
    public void execute(Tuple tuple) {
        if (handleRejectedTuple(tuple)) return;

        long ts = tuple.getLongByField("ts");
        String deviceId = tuple.getStringByField("device");
        double co = tuple.getDoubleByField("co");
        double humidity = tuple.getDoubleByField("humidity");
        boolean light = tuple.getBooleanByField("light");
        double lpg = tuple.getDoubleByField("lpg");
        double smoke = tuple.getDoubleByField("smoke");
        double temp = tuple.getDoubleByField("temp");

        checkThresholdsAndSendAlert("co", deviceId, ts, co, thresholds.coLow(), thresholds.coHigh());
        checkThresholdsAndSendAlert("humidity", deviceId, ts, humidity, thresholds.humidityLow(), thresholds.humidityHigh());
        checkThresholdsAndSendAlert("light", deviceId, ts, light ? 1.0 : 0.0, thresholds.lpgLow(), thresholds.lpgHigh());
        checkThresholdsAndSendAlert("lpg", deviceId, ts, lpg, thresholds.lpgLow(), thresholds.lpgHigh());
        checkThresholdsAndSendAlert("smoke", deviceId, ts, smoke, thresholds.smokeLow(), thresholds.smokeHigh());
        checkThresholdsAndSendAlert("temp", deviceId, ts, temp, thresholds.tempLow(), thresholds.tempHigh());
    }

    private boolean handleRejectedTuple(Tuple tuple) {
        return tuple.getBooleanByField("rejected");
    }

    private void checkThresholdsAndSendAlert(String sensorName, String deviceId, long ts, double value, double low, double high) {
        SensorWindow window = sensorWindows.get(sensorName);

        if (value < low || value > high) {
            if (!window.thresholdExceeded) {
                window.thresholdExceeded = true;
                window.anomalyStart = ts;
                emitAlert(sensorName, deviceId, ts, value, "anomaly_start");
            }
        } else {
            if (window.thresholdExceeded) {
                window.thresholdExceeded = false;
                emitAlert(sensorName, deviceId, ts, value, "anomaly_end");
            }
        }
    }

    private void emitAlert(String sensorName, String deviceId, long ts, double value, String eventType) {
        outputCollector.emit("alertStream", new Values(deviceId, eventType, sensorName, ts, false, value));
        log.info("Emitting {} alert for sensor {} of device {}: {}", eventType, sensorName, deviceId, ts);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("alertStream", new Fields("deviceId", "eventType", "sensorType", "timestamp", "isSuspicious", "value"));
    }
}

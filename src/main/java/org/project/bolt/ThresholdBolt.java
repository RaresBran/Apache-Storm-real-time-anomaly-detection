package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.project.utility.Thresholds;
import org.project.handler.RedisHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class ThresholdBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(ThresholdBolt.class);
    private transient OutputCollector outputCollector;
    private Map<String, SensorWindow> sensorWindows;
    private RedisHandler redisHandler;
    private final String redisHost;
    private final int redisPort;
    private Thresholds thresholds;

    private static class SensorWindow implements Serializable {
        boolean thresholdExceeded;
        long anomalyStart;

    }

    public ThresholdBolt(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.redisHandler = new RedisHandler(redisHost, redisPort);

        sensorWindows = new HashMap<>();
        sensorWindows.put("co", new SensorWindow());
        sensorWindows.put("humidity", new SensorWindow());
        sensorWindows.put("lpg", new SensorWindow());
        sensorWindows.put("smoke", new SensorWindow());
        sensorWindows.put("temp", new SensorWindow());

        fetchThresholds();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                fetchThresholds();
            }
        }, 0, 10000); // Fetch thresholds every 10 seconds
    }

    private void fetchThresholds() {
        thresholds = redisHandler.getThresholds("Thresholds:default");
    }

    @Override
    public void execute(Tuple tuple) {
        if (handleRejectedTuple(tuple)) return;

        long ts = tuple.getLongByField("ts");
        String deviceId = tuple.getStringByField("device");
        double co = tuple.getDoubleByField("co");
        double humidity = tuple.getDoubleByField("humidity");
        double lpg = tuple.getDoubleByField("lpg");
        double smoke = tuple.getDoubleByField("smoke");
        double temp = tuple.getDoubleByField("temp");
        boolean suspicious = tuple.getBooleanByField("suspicious");

        checkThresholdsAndSendAlert("co", deviceId, ts, co, thresholds.coLower(), thresholds.coUpper(), suspicious);
        checkThresholdsAndSendAlert("humidity", deviceId, ts, humidity, thresholds.humidityLower(), thresholds.humidityUpper(),suspicious);
        checkThresholdsAndSendAlert("lpg", deviceId, ts, lpg, thresholds.lpgLower(), thresholds.lpgUpper(), suspicious);
        checkThresholdsAndSendAlert("smoke", deviceId, ts, smoke, thresholds.smokeLower(), thresholds.smokeUpper(), suspicious);
        checkThresholdsAndSendAlert("temp", deviceId, ts, temp, thresholds.temperatureLower(), thresholds.temperatureUpper(), suspicious);

        outputCollector.ack(tuple);
    }

    private boolean handleRejectedTuple(Tuple tuple) {
        return tuple.getBooleanByField("rejected");
    }

    private void checkThresholdsAndSendAlert(String sensorName, String deviceId, long ts, double value, double low, double high, boolean suspicious) {
        SensorWindow window = sensorWindows.get(sensorName);

        if (value < low || value > high) {
            if (!window.thresholdExceeded) {
                window.thresholdExceeded = true;
                window.anomalyStart = ts;
                emitAlert(sensorName, deviceId, ts, value, suspicious, "anomaly_start");
            }
        } else if (window.thresholdExceeded) {
                window.thresholdExceeded = false;
                emitAlert(sensorName, deviceId, ts, value, suspicious, "anomaly_end");
        }
    }

    private void emitAlert(String sensorName, String deviceId, long ts, double value, boolean suspicious, String eventType) {
        outputCollector.emit("alertStream", new Values(deviceId, eventType, sensorName, ts, suspicious, value));
        log.info("Emitting {} alert for sensor {} of device {}: {}", eventType, sensorName, deviceId, ts);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("alertStream", new Fields("deviceId", "eventType", "sensorType", "timestamp", "suspicious", "value"));
    }
}

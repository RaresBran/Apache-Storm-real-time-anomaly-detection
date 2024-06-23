package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.project.handler.AlertHandler;
import org.project.handler.RedisHandler;
import org.project.handler.TimescaleDBHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AlertBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(AlertBolt.class);
    private transient OutputCollector collector;
    private AlertHandler alertHandler;
    private TimescaleDBHandler timescaleDBHandler;

    private final String timescaleUrl;
    private final String user;
    private final String password;
    private final String redisHost;
    private final int redisPort;

    public AlertBolt(String timescaleUrl, String user, String password, String redisHost, int redisPort) {
        this.timescaleUrl = timescaleUrl;
        this.user = user;
        this.password = password;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.alertHandler = new AlertHandler(new RedisHandler(redisHost, redisPort));
        this.timescaleDBHandler = new TimescaleDBHandler(timescaleUrl, user, password);
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String deviceId = tuple.getStringByField("deviceId");
        String eventType = tuple.getStringByField("eventType");
        String sensorType = tuple.getStringByField("sensorType");
        long timestamp = tuple.getLongByField("timestamp");
        boolean isSuspicious = tuple.getBooleanByField("suspicious");
        double value = tuple.getDoubleByField("value");

        timescaleDBHandler.saveAlert(deviceId, eventType, sensorType, timestamp, isSuspicious, value);
        sendEmailAlert(deviceId, eventType, sensorType, timestamp, isSuspicious, value);
        collector.ack(tuple);
    }

    private void sendEmailAlert(String deviceId, String eventType, String sensorType, long timestamp, boolean isSuspicious, double value) {
        String formattedTimestamp = getFormattedTimestamp(timestamp);
        if (eventType.equals("anomaly_start"))
            eventType = "Anomaly start";
        else if (eventType.equals("anomaly_end"))
            eventType = "Anomaly end";

        String message = String.format("""
                This is an automated message informing you that there has been an `%s` event for sensor `%s` on device `%s`.
                Timestamp of the event:            %s
                Value that triggered the event:    %.2f
                Can be false flag:                 %b
                
                Please take action!
                """,
                eventType, sensorType, deviceId, formattedTimestamp, value, isSuspicious);

        alertHandler.sendEmailAlert(sensorType + " " + eventType + " Alert", message);
        log.info("Email alert sent for device {}: {}", deviceId, message);
    }

    private String getFormattedTimestamp(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void cleanup() {
        timescaleDBHandler.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // No output fields
    }
}

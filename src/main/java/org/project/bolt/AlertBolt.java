package org.project.bolt;

import com.influxdb.client.domain.WritePrecision;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.project.service.AlertService;
import org.project.service.InfluxDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.client.write.Point;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AlertBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(AlertBolt.class);
    private transient AlertService alertService;
    private transient InfluxDBService influxDBService;

    private final String influxDbUrl;
    private final String bucket;
    private final String org;
    private final String token;

    public AlertBolt(String influxDbUrl, String bucket, String org, String token) {
        this.influxDbUrl = influxDbUrl;
        this.bucket = bucket;
        this.org = org;
        this.token = token;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.alertService = new AlertService();
        this.influxDBService = new InfluxDBService(influxDbUrl, bucket, org, token);
    }

    @Override
    public void execute(Tuple tuple) {
        String deviceId = tuple.getStringByField("deviceId");
        String eventType = tuple.getStringByField("eventType");
        String sensorType = tuple.getStringByField("sensorType");
        long timestamp = tuple.getLongByField("timestamp");
        boolean isSuspicious = tuple.getBooleanByField("isSuspicious");
        double value = tuple.getDoubleByField("value");

        sendEmailAlert(deviceId, eventType, sensorType, timestamp, isSuspicious, value);
        saveAlertToInfluxDB(deviceId, eventType, sensorType, timestamp, isSuspicious, value);
    }

    private void sendEmailAlert(String deviceId, String eventType, String sensorType, long timestamp, boolean isSuspicious, double value) {
        String formattedTimestamp = formatTimestamp(timestamp);
        String message = String.format("%s event for %s on device %s%nTimestamp: %s%nValue: %.2f%nSuspicious: %b",
                eventType, sensorType, deviceId, formattedTimestamp, value, isSuspicious);

        alertService.sendEmailAlert(sensorType + " " + eventType + " Alert", message);
        log.info("Email alert sent for device {}: {}", deviceId, message);
    }

    private void saveAlertToInfluxDB(String deviceId, String eventType, String sensorType, long timestamp, boolean isSuspicious, double value) {
        Point point = Point
                .measurement("event_alerts")
                .addTag("device", deviceId)
                .addField("eventType", eventType)
                .addField("sensorType", sensorType)
                .addField("value", value)
                .addField("suspicious", isSuspicious)
                .time(timestamp, WritePrecision.MS);

        influxDBService.writePoint(point);
    }

    private String formatTimestamp(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void cleanup() {
        if (influxDBService != null) {
            influxDBService.close();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // No output fields
    }
}

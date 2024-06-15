package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.project.service.AlertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AlertBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(AlertBolt.class);
    private transient AlertService alertService;
    private transient Connection connection;

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public AlertBolt(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.alertService = new AlertService();
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        } catch (SQLException e) {
            throw new RuntimeException("Error initializing database connection", e);
        }
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
        saveAlertToTimescaleDB(deviceId, eventType, sensorType, timestamp, isSuspicious, value);
    }

    private void sendEmailAlert(String deviceId, String eventType, String sensorType, long timestamp, boolean isSuspicious, double value) {
        String formattedTimestamp = formatTimestamp(timestamp);
        String message = String.format("%s event for %s on device %s%nTimestamp: %s%nValue: %.2f%nSuspicious: %b",
                eventType, sensorType, deviceId, formattedTimestamp, value, isSuspicious);

        alertService.sendEmailAlert(sensorType + " " + eventType + " Alert", message);
        log.info("Email alert sent for device {}: {}", deviceId, message);
    }

    private void saveAlertToTimescaleDB(String deviceId, String eventType, String sensorType, long timestamp, boolean isSuspicious, double value) {
        String sql = "INSERT INTO event_alerts (time, device, event_type, sensor_type, value, suspicious) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(timestamp));
            statement.setString(2, deviceId);
            statement.setString(3, eventType);
            statement.setString(4, sensorType);
            statement.setDouble(5, value);
            statement.setBoolean(6, isSuspicious);

            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement", e);
        }
    }

    private String formatTimestamp(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Log and handle the error properly
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // No output fields
    }
}

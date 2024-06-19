package org.project.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimescaleDBHandler {
    private static final Logger log = LoggerFactory.getLogger(TimescaleDBHandler.class);
    private Connection connection;

    public TimescaleDBHandler(String jdbcUrl, String user, String password) {
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, user, password);
            this.connection.setAutoCommit(false); // Enable transaction control
        } catch (SQLException e) {
            log.error("Error initializing database connection", e);
        }
    }

    public void saveSensorData(long ts, String device, double temp, double humidity, double co, boolean light, double lpg, boolean motion, double smoke, boolean rejected, boolean suspicious) {
        String sql = "INSERT INTO sensor_data (time, device, temperature, humidity, co, light, lpg, motion, smoke, rejected, suspicious) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(ts));
            statement.setString(2, device);
            statement.setDouble(3, temp);
            statement.setDouble(4, humidity);
            statement.setDouble(5, co);
            statement.setInt(6, light ? 1 : 0);
            statement.setDouble(7, lpg);
            statement.setInt(8, motion ? 1 : 0);
            statement.setDouble(9, smoke);
            statement.setBoolean(10, rejected);
            statement.setBoolean(11, suspicious);

            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("Error during rollback", ex);
            }
            log.error("Error executing SQL statement", e);
        }
    }

    public void saveAlert(String deviceId, String eventType, String sensorType, long timestamp, boolean isSuspicious, double value) {
        String sql = "INSERT INTO event_alerts (time, device, event_type, sensor_type, value, suspicious) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(timestamp));
            statement.setString(2, deviceId);
            statement.setString(3, eventType);
            statement.setString(4, sensorType);
            statement.setDouble(5, value);
            statement.setBoolean(6, isSuspicious);

            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("Error during rollback", ex);
            }
            log.error("Error executing SQL statement", e);
        }
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Error closing connection", e);
            }
        }
    }
}

package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.*;
import java.util.Map;

public class TimescaleDBBolt extends BaseRichBolt {
    private transient OutputCollector collector;
    private transient Connection connection;
    private transient PreparedStatement statement;
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public TimescaleDBBolt(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, user, password);
            this.connection.setAutoCommit(false); // Enable transaction control
            String sql = "INSERT INTO sensor_data (time, device, temperature, humidity, co, light, lpg, motion, smoke, rejected, suspicious) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            this.statement = connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Error initializing database connection", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            long ts = tuple.getLongByField("ts");  // Assuming this returns epoch milliseconds
            Timestamp timestamp = new Timestamp(ts);

            statement.setTimestamp(1, timestamp);
            statement.setString(2, tuple.getStringByField("device"));
            statement.setDouble(3, tuple.getDoubleByField("temp"));
            statement.setDouble(4, tuple.getDoubleByField("humidity"));
            statement.setDouble(5, tuple.getDoubleByField("co"));
            statement.setInt(6, tuple.getBooleanByField("light") ? 1 : 0);
            statement.setDouble(7, tuple.getDoubleByField("lpg"));
            statement.setInt(8, tuple.getBooleanByField("motion") ? 1 : 0);
            statement.setDouble(9, tuple.getDoubleByField("smoke"));
            statement.setBoolean(10, tuple.getBooleanByField("rejected"));
            statement.setBoolean(11, tuple.getBooleanByField("suspicious"));

            statement.executeUpdate();
            connection.commit();
            collector.ack(tuple);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                // Handle rollback exception
            }
            collector.fail(tuple);
            throw new RuntimeException("Error executing SQL statement", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare
    }

    @Override
    public void cleanup() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // Log and handle the error properly
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Log and handle the error properly
            }
        }
    }
}

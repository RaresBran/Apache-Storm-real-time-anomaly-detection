package org.project.bolt;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class InfluxDBBolt extends BaseRichBolt {
    private transient InfluxDBClient influxDBClient;
    private transient OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String influxDbUrl = "http://localhost:8086"; // URL to InfluxDB
        String bucket = "sensor-data"; // Database name
        String org = "student";
        String token = "wxeq02AqsDd8Qj8g9ICn1pvHQypGyblvDJQn1HldwXSZweOTw0-cyc7I6NVqmaeG1qk4ZLpQ3BzpiiQsSdwAuA==";
        this.influxDBClient = InfluxDBClientFactory.create(influxDbUrl, token.toCharArray(), org, bucket);
    }

    @Override
    public void execute(Tuple tuple) {
        // Extract data from the tuple
        long timestamp = tuple.getLongByField("ts");
        String device = tuple.getStringByField("device");
        double co = tuple.getDoubleByField("co");
        double humidity = tuple.getDoubleByField("humidity");
        boolean light = tuple.getBooleanByField("light");
        double lpg = tuple.getDoubleByField("lpg");
        boolean motion = tuple.getBooleanByField("motion");
        double smoke = tuple.getDoubleByField("smoke");
        double temp = tuple.getDoubleByField("temp");

        // Create a point and write it to InfluxDB
        Point point = Point
                .measurement("sensor_measurement")
                .addTag("device", device)
                .addField("temperature", temp)
                .addField("humidity", humidity)
                .addField("co", co)
                .addField("light", light ? 1 : 0)  // Storing boolean as integer
                .addField("lpg", lpg)
                .addField("motion", motion ? 1 : 0)  // Storing boolean as integer
                .addField("smoke", smoke)
                .time(timestamp, WritePrecision.MS);

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        writeApi.writePoint(point);

        // Acknowledge the tuple
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare
    }

    @Override
    public void cleanup() {
        if (influxDBClient != null) {
            influxDBClient.close();
        }
    }
}

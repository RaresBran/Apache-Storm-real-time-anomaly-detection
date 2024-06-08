package org.project.bolt;

import com.influxdb.client.domain.WritePrecision;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.project.service.InfluxDBService;
import com.influxdb.client.write.Point;

import java.util.Map;

public class InfluxDBBolt extends BaseRichBolt {
    private transient OutputCollector collector;
    private transient InfluxDBService influxDBService;
    private final String influxDbUrl;
    private final String bucket;
    private final String org;
    private final String token;

    public InfluxDBBolt(String influxDbUrl, String bucket, String org, String token) {
        this.influxDbUrl = influxDbUrl;
        this.bucket = bucket;
        this.org = org;
        this.token = token;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.influxDBService = new InfluxDBService(influxDbUrl, bucket, org, token);
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
        boolean rejected  = tuple.getBooleanByField("rejected");
        boolean suspicious  = tuple.getBooleanByField("suspicious");

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
                .addField("rejected", rejected)
                .addField("suspicious", suspicious)
                .time(timestamp, WritePrecision.MS);

        influxDBService.writePoint(point);

        // Acknowledge the tuple
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare
    }

    @Override
    public void cleanup() {
        if (influxDBService != null) {
            influxDBService.close();
        }
    }
}

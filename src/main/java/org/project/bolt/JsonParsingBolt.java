package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonParsingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final Logger log = LoggerFactory.getLogger(JsonParsingBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String message = input.getStringByField("value");
        try {
            JSONObject json = new JSONObject(message);

            long ts = json.getLong("ts");
            String device = json.getString("device");
            double co = json.getDouble("co");
            double humidity = json.getDouble("humidity");
            boolean light = json.getBoolean("light");
            double lpg = json.getDouble("lpg");
            boolean motion = json.getBoolean("motion");
            double smoke = json.getDouble("smoke");
            double temp = json.getDouble("temp");
            boolean rejected = false;
            boolean suspicious = false;

            collector.emit(new Values(ts, device, co, humidity, light, lpg, motion, smoke, temp, rejected, suspicious));
        } catch (Exception e) {
            log.error("Error parsing JSON message: {}", message, e);
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected", "suspicious"));
    }
}

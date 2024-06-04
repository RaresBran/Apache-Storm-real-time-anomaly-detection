package org.project.bolt.cleaning;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class NullValueBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(NullValueBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long ts = input.getLongByField("ts");
        String device = input.getStringByField("device");

        Map<String, Double> sensorData = new HashMap<>();
        sensorData.put("co", input.getDoubleByField("co"));
        sensorData.put("humidity", input.getDoubleByField("humidity"));
        sensorData.put("lpg", input.getDoubleByField("lpg"));
        sensorData.put("smoke", input.getDoubleByField("smoke"));
        sensorData.put("temp", input.getDoubleByField("temp"));

        boolean rejected = false;

        for (Map.Entry<String, Double> entry : sensorData.entrySet()) {
            if (Double.isNaN(entry.getValue())) {
                // Reject null values
                log.warn("NaN value detected");
                rejected = true;
                break;
            }
        }

        // Emit cleaned data
        collector.emit(new Values(
                ts,
                device,
                sensorData.get("co"),
                sensorData.get("humidity"),
                input.getBooleanByField("light"),
                sensorData.get("lpg"),
                input.getBooleanByField("motion"),
                sensorData.get("smoke"),
                sensorData.get("temp"),
                rejected
        ));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "rejected"));
    }
}

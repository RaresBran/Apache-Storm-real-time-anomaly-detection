package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.project.handler.TimescaleDBHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TimescaleDBBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(TimescaleDBBolt.class);
    private OutputCollector collector;
    private TimescaleDBHandler timescaleDBHandler;
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public TimescaleDBBolt(String timescaleUrl, String timescaleUser, String timescalePass) {
        this.jdbcUrl = timescaleUrl;
        this.user = timescaleUser;
        this.password = timescalePass;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.timescaleDBHandler = new TimescaleDBHandler(jdbcUrl, user, password);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            long ts = tuple.getLongByField("ts");  // Assuming this returns epoch milliseconds

            timescaleDBHandler.saveSensorData(
                    ts,
                    tuple.getStringByField("device"),
                    tuple.getDoubleByField("temp"),
                    tuple.getDoubleByField("humidity"),
                    tuple.getDoubleByField("co"),
                    tuple.getBooleanByField("light"),
                    tuple.getDoubleByField("lpg"),
                    tuple.getBooleanByField("motion"),
                    tuple.getDoubleByField("smoke"),
                    tuple.getBooleanByField("rejected"),
                    tuple.getBooleanByField("suspicious")
            );

            collector.ack(tuple);
        } catch (Exception e) {
            collector.fail(tuple);
            log.error("Error processing tuple", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare
    }

    @Override
    public void cleanup() {
        timescaleDBHandler.close();
    }
}

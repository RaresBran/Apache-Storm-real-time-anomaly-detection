package org.project.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintingBolt extends BaseBasicBolt {
    private static final Logger log = LoggerFactory.getLogger(PrintingBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        log.info("{}", tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //
    }
}

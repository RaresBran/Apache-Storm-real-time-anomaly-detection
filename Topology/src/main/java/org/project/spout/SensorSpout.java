package org.project.spout;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class SensorSpout extends BaseRichSpout {
    public static final int SENSOR_DATA_INTERVAL = 1000;

    private transient SpoutOutputCollector collector;
    private transient CSVParser csvParser;
    private final String filterDeviceId;
    private final String filename;

    public SensorSpout(String deviceId, String filename) {
        this.filterDeviceId = deviceId;
        this.filename = filename;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.csvParser = createParser(this.filename);
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (csvParser.iterator().hasNext()) {
            CSVRecord csvRecord = csvParser.iterator().next();
            if (csvRecord.get("device").equals(filterDeviceId)) {
                long ts = System.currentTimeMillis();
                String device = csvRecord.get(1);
                double co = Double.parseDouble(csvRecord.get(2));
                double humidity = Double.parseDouble(csvRecord.get(3));
                boolean light = Boolean.parseBoolean(csvRecord.get(4));
                double lpg = Double.parseDouble(csvRecord.get(5));
                boolean motion = Boolean.parseBoolean(csvRecord.get(6));
                double smoke = Double.parseDouble(csvRecord.get(7));
                double temp = Double.parseDouble(csvRecord.get(8));
                collector.emit(new Values(ts, device, co, humidity, light, lpg, motion, smoke, temp));
                Utils.sleep(SENSOR_DATA_INTERVAL);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp"));
    }

    private CSVParser createParser(String filename) {
        try {
            Reader reader = new FileReader(filename);
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader() // Use this if your data has a header row
                    .setIgnoreHeaderCase(true)
                    .setTrim(true)
                    .build();
            return new CSVParser(reader, csvFormat);
        } catch (IOException e) {
            throw new RuntimeException("Error reading sensor data file", e);
        }
    }
}

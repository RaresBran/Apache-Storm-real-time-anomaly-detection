package org.project.trident;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TridentSensorSpout implements IBatchSpout {
    public static final int SENSOR_DATA_INTERVAL = 1000;

    private final String filename;
    private transient CSVParser csvParser;
    private final transient Map<Long, CSVParser> batchParserMap;
    private final String filterDeviceId;
    private final int batchSize;

    public TridentSensorSpout(String deviceId, String filename, int batchSize) {
        this.filterDeviceId = deviceId;
        this.filename = filename;
        this.batchSize = batchSize;
        this.batchParserMap = new HashMap<>();
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext) {
        this.csvParser = createParser();
    }

    @Override
    public void emitBatch(long batchId, TridentCollector tridentCollector) {
        CSVParser parser = batchParserMap.getOrDefault(batchId, createParser());
        int emitted = 0;
        while (emitted < batchSize && parser.iterator().hasNext()) {
            CSVRecord csvRecord = parser.iterator().next();
            if (csvRecord.get("device").equals(filterDeviceId)) {
                String device = csvRecord.get(1);
                double co = Double.parseDouble(csvRecord.get(2));
                double humidity = Double.parseDouble(csvRecord.get(3));
                boolean light = Boolean.parseBoolean(csvRecord.get(4));
                double lpg = Double.parseDouble(csvRecord.get(5));
                boolean motion = Boolean.parseBoolean(csvRecord.get(6));
                double smoke = Double.parseDouble(csvRecord.get(7));
                double temp = Double.parseDouble(csvRecord.get(8));
                LocalDateTime ts = LocalDateTime.now();
                tridentCollector.emit(new Values(ts, device, co, humidity, light, lpg, motion, smoke, temp));
                emitted++;
                try {
                    Thread.sleep(SENSOR_DATA_INTERVAL);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        batchParserMap.put(batchId, parser); // Save the parser state
    }

    private CSVParser createParser() {
        try {
            Reader reader = new FileReader(this.filename);
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

    @Override
    public void ack(long batchId) {
        batchParserMap.remove(batchId);
    }

    @Override
    public void close() {
        if (this.csvParser != null) {
            try {
                csvParser.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Collections.emptyMap();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp");
    }
}

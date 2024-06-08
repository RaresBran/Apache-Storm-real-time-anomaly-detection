package org.project;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.project.bolt.*;
import org.project.bolt.cleaning.*;
import org.project.spout.KafkaSpoutConfigBuilder;

import java.util.HashMap;
import java.util.Map;

public class SensorTopology extends ConfigurableTopology {
    private static final Logger log = LoggerFactory.getLogger(SensorTopology.class);
    public static final String ALERT_STREAM = "alertStream";

    public static void main(String[] args) {
        ConfigurableTopology.start(new SensorTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        if (args.length < 5) {
            throw new IllegalArgumentException("Usage: " +
                    "SensorTopology " +
                    "<kafka-broker-url> " +
                    "<influxdb-url> " +
                    "<influxdb-bucket> " +
                    "<influxdb-organization> " +
                    "<influxdb-token>"
            );
        }

        String kafkaBrokerUrl = args[0];
        String influxdbUrl = args[1];
        String influxdbBucket = args[2];
        String influxdbOrganization = args[3];
        String influxdbToken = args[4];
        String[] devices = {"b8:27:eb:bf:9d:51", "00:0f:00:70:91:0a", "1c:bf:ce:15:ec:4d"};
        String[] topics = {"sensor1-data", "sensor2-data", "sensor3-data"};

        TopologyBuilder builder = new TopologyBuilder();

        for (int i = 0; i < devices.length; i++) {
            String deviceId = devices[i];
            String topic = topics[i];

            // Data spouts and emitting
            KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfigBuilder.createKafkaSpoutConfig(kafkaBrokerUrl, topic);
            builder.setSpout("kafka-spout-" + deviceId, new KafkaSpout<>(kafkaSpoutConfig));
            builder.setBolt("json-parsing-bolt-" + deviceId, new JsonParsingBolt())
                    .shuffleGrouping("kafka-spout-" + deviceId);

            // Data cleaning
            builder.setBolt("bad-timestamps-bolt-" + deviceId, new BadTimestampBolt())
                    .shuffleGrouping("json-parsing-bolt-" + deviceId);
            builder.setBolt("data-outliers-bolt-" + deviceId, new DataOutlierBolt())
                    .shuffleGrouping("bad-timestamps-bolt-" + deviceId);
            builder.setBolt("false-spikes-bolt-" + deviceId, new FalseSpikeBolt())
                    .shuffleGrouping("data-outliers-bolt-" + deviceId);

            // Alert detection bolts
            builder.setBolt("value-blocked-bolt-" + deviceId, new ValueBlockedBolt(12 * 60 * 60 * 1000L))
                    .shuffleGrouping("false-spikes-bolt-" + deviceId);
            builder.setBolt("threshold-bolt-" + deviceId, new ThresholdBolt(getThresholdsMap()))
                    .shuffleGrouping("value-blocked-bolt-" + deviceId);

            // Save to database bolt
            builder.setBolt("print-bolt-" + deviceId, tuple -> log.info(tuple.toString()))
                    .shuffleGrouping("value-blocked-bolt-" + deviceId);
            builder.setBolt("influxdb-bolt-" + deviceId, new InfluxDBBolt(influxdbUrl, influxdbBucket, influxdbOrganization, influxdbToken))
                    .shuffleGrouping("value-blocked-bolt-" + deviceId);

            // Alert emitting bolt
            builder.setBolt("alert-bolt-" + deviceId, new AlertBolt(influxdbUrl, influxdbBucket, influxdbOrganization, influxdbToken))
                    .shuffleGrouping("threshold-bolt-" + deviceId, ALERT_STREAM)
                    .shuffleGrouping("value-blocked-bolt-" + deviceId, ALERT_STREAM);
        }

//        Config conf = new Config();
//        conf.setDebug(false);
//        conf.setNumWorkers(2);  // Set the number of worker processes
//
//        StormSubmitter.submitTopology("sensor-topology", conf, builder.createTopology());
//        return 0;

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sensor-topology", conf, builder.createTopology());
        Utils.sleep(1000000);
        cluster.shutdown();
        return 0;
    }

    private static @NotNull Map<String, Double> getThresholdsMap() {
        Map<String, Double> thresholdsMap = new HashMap<>();
        thresholdsMap.put("coLow", 0.0);
        thresholdsMap.put("coHigh", 10.0);
        thresholdsMap.put("humidityLow", 0.0);
        thresholdsMap.put("humidityHigh", 100.0);
        thresholdsMap.put("lpgLow", 0.0);
        thresholdsMap.put("lpgHigh", 1000.0);
        thresholdsMap.put("smokeLow", 0.0);
        thresholdsMap.put("smokeHigh", 5.0);
        thresholdsMap.put("tempLow", -40.0);
        thresholdsMap.put("tempHigh", 25.0);
        return thresholdsMap;
    }
}

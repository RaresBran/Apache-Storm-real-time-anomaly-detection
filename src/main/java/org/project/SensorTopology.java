package org.project;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.project.bolt.*;
import org.project.bolt.cleaning.*;
import org.project.spout.KafkaSpoutConfigBuilder;

public class SensorTopology extends ConfigurableTopology {
    private static final Logger log = LoggerFactory.getLogger(SensorTopology.class);
    public static final String ALERT_STREAM = "alertStream";

    public static void main(String[] args) {
        ConfigurableTopology.start(new SensorTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        if (args.length < 6) {
            throw new IllegalArgumentException("Usage: " +
                    "SensorTopology " +
                    "<kafka-broker-url> " +
                    "<influxdb-url> " +
                    "<redis-url>" +
                    "<influxdb-bucket> " +
                    "<influxdb-organization> " +
                    "<influxdb-token> "
            );
        }

        String kafkaBrokerUrl = args[0];
        String influxdbUrl = args[1];
        String redisUrl = args[2];
        String influxdbBucket = args[3];
        String influxdbOrganization = args[4];
        String influxdbToken = args[5];

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

            // Parse Redis URL
            String[] redisParts = redisUrl.split(":");
            String redisHost = redisParts[0];
            int redisPort = Integer.parseInt(redisParts[1]);
            builder.setBolt("threshold-bolt-" + deviceId, new ThresholdBolt(redisHost, redisPort))
                    .shuffleGrouping("value-blocked-bolt-" + deviceId);

            // Save to database bolt
            builder.setBolt("print-bolt-" + deviceId, tuple -> log.info(tuple.toString()))
                    .shuffleGrouping("value-blocked-bolt-" + deviceId);
            builder.setBolt("influxdb-bolt-" + deviceId,
                            new InfluxDBBolt(influxdbUrl, influxdbBucket, influxdbOrganization, influxdbToken))
                    .shuffleGrouping("value-blocked-bolt-" + deviceId);

            // Alert emitting bolt
            builder.setBolt("alert-bolt-" + deviceId,
                            new AlertBolt(influxdbUrl, influxdbBucket, influxdbOrganization, influxdbToken))
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
}

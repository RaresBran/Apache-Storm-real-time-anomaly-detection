package org.project;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.project.bolt.*;
import org.project.bolt.cleaning.*;
import org.project.spout.KafkaSpoutConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SensorTopology extends ConfigurableTopology {
    private static final String ALERT_STREAM = "alertStream";
    private static final String DEVICE_ID_FIELD = "device";
    private static final String[] DEVICES = {"b8:27:eb:bf:9d:51", "00:0f:00:70:91:0a", "1c:bf:ce:15:ec:4d"};
    private static final String[] TOPICS = {"sensor1-data", "sensor2-data", "sensor3-data"};
    private static final Logger log = LoggerFactory.getLogger(SensorTopology.class);

    public static void main(String[] args) {
        ConfigurableTopology.start(new SensorTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        if (args.length < 5) {
            throw new IllegalArgumentException("Usage: " +
                    "SensorTopology " +
                    "<kafka-broker-url> " +
                    "<redis-url> " +
                    "<timescale-url> " +
                    "<timescale-username> " +
                    "<timescale-password> "
            );
        }

        String kafkaBrokerUrl = args[0];
        String redisUrl = args[1];
        String timescaleUrl = args[2];
        String timescaleUser = args[3];
        String timescalePass = args[4];

        // Parse Redis URL
        String[] redisParts = redisUrl.split(":");
        String redisHost = redisParts[0];
        int redisPort = Integer.parseInt(redisParts[1]);

        TopologyBuilder builder = new TopologyBuilder();

        for (int i = 0; i < DEVICES.length; i++) {
            String deviceId = DEVICES[i];
            String topic = TOPICS[i];

            // Data spouts and emitting
            KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfigBuilder.createKafkaSpoutConfig(kafkaBrokerUrl, topic);
            builder.setSpout("kafka-spout-" + deviceId, new KafkaSpout<>(kafkaSpoutConfig));
        }

        builder.setBolt("input-parsing-bolt", new InputParsingBolt(), 3)
                .shuffleGrouping("kafka-spout-" + DEVICES[0])
                .shuffleGrouping("kafka-spout-" + DEVICES[1])
                .shuffleGrouping("kafka-spout-" + DEVICES[2]);

        builder.setBolt("bad-timestamps-bolt",
                        new BadTimestampBolt()
                                .withWindow(BaseWindowedBolt.Count.of(3), BaseWindowedBolt.Count.of(1)), 3)
                .fieldsGrouping("input-parsing-bolt", new Fields(DEVICE_ID_FIELD));

        builder.setBolt("data-outliers-bolt", new DataOutlierBolt(), 3)
                .fieldsGrouping("bad-timestamps-bolt", new Fields(DEVICE_ID_FIELD));

        builder.setBolt("false-spikes-bolt", new FalseSpikeBolt(), 3)
                .fieldsGrouping("data-outliers-bolt", new Fields(DEVICE_ID_FIELD));

        builder.setBolt("value-blocked-bolt", new ValueBlockedBolt(12 * 60 * 60 * 1000L), 3)
                .fieldsGrouping("false-spikes-bolt", new Fields(DEVICE_ID_FIELD));

        builder.setBolt("threshold-bolt", new ThresholdBolt(redisHost, redisPort), 3)
                .fieldsGrouping("value-blocked-bolt", new Fields(DEVICE_ID_FIELD));

        builder.setBolt("print-bolt", tuple -> log.info(tuple.toString()))
                .shuffleGrouping("value-blocked-bolt");

        builder.setBolt("timescaledb-bolt",
                        new TimescaleDBBolt(timescaleUrl, timescaleUser, timescalePass), 3)
                .fieldsGrouping("value-blocked-bolt", new Fields(DEVICE_ID_FIELD));

        builder.setBolt("alert-bolt",
                        new AlertBolt(timescaleUrl, timescaleUser, timescalePass, redisHost, redisPort))
                .shuffleGrouping("threshold-bolt", ALERT_STREAM);

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

package org.project;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.project.bolt.InfluxDBBolt;
import org.project.bolt.JsonParsingBolt;
import org.project.bolt.AlertBolt;
import org.project.bolt.cleaning.*;
import org.project.spout.KafkaSpoutConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SensorTopology extends ConfigurableTopology {
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String[] DEVICES = {"b8:27:eb:bf:9d:51", "00:0f:00:70:91:0a", "1c:bf:ce:15:ec:4d"};
    private static final String[] TOPICS = {"sensor1-data", "sensor2-data", "sensor3-data"};
    private static final Logger log = LoggerFactory.getLogger(SensorTopology.class);

    public static void main(String[] args) {
        ConfigurableTopology.start(new SensorTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        for (int i = 0; i < DEVICES.length; i++) {
            String deviceId = DEVICES[i];
            String topic = TOPICS[i];

            KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfigBuilder.createKafkaSpoutConfig(KAFKA_BROKER, topic);
            builder.setSpout("kafka-spout-" + deviceId, new KafkaSpout<>(kafkaSpoutConfig));

            builder.setBolt("json-parsing-bolt-" + deviceId, new JsonParsingBolt()).shuffleGrouping("kafka-spout-" + deviceId);
            builder.setBolt("bad-timestamps-bolt-" + deviceId, new BadTimestampBolt()).shuffleGrouping("json-parsing-bolt-" + deviceId);
            builder.setBolt("data-gaps-bolt-" + deviceId, new DataGapBolt()).shuffleGrouping("bad-timestamps-bolt-" + deviceId);
            builder.setBolt("null-values-bolt-" + deviceId, new NullValueBolt()).shuffleGrouping("data-gaps-bolt-" + deviceId);
            builder.setBolt("data-outliers-bolt-" + deviceId, new DataOutlierBolt()).shuffleGrouping("null-values-bolt-" + deviceId);
            builder.setBolt("false-spikes-bolt-" + deviceId, new FalseSpikeBolt()).shuffleGrouping("data-outliers-bolt-" + deviceId);
            builder.setBolt("mean-shift-bolt-" + deviceId, new MeanShiftBolt()).shuffleGrouping("false-spikes-bolt-" + deviceId);
            builder.setBolt("data-blocked-bolt-" + deviceId, new DataBlockedBolt()).shuffleGrouping("mean-shift-bolt-" + deviceId);

            builder.setBolt("alert-bolt-" + deviceId, new AlertBolt()).shuffleGrouping("data-blocked-bolt-" + deviceId);
            builder.setBolt("print-bolt-" + deviceId, tuple -> log.info(tuple.toString())).shuffleGrouping("data-blocked-bolt-" + deviceId);
            builder.setBolt("influxdb-bolt-" + deviceId, new InfluxDBBolt()).shuffleGrouping("data-blocked-bolt-" + deviceId);
        }

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        if (args != null && args.length > 0) {
            return submit(args[0], conf, builder);
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("sensor-topology", conf, builder.createTopology());
            Utils.sleep(1000000);
            cluster.shutdown();
            return 0;
        }
    }
}

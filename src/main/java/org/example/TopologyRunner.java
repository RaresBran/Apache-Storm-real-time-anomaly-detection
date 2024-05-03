package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.example.bolt.AggregatingBolt;
import org.example.bolt.FileWritingBolt;
import org.example.bolt.FilteringBolt;
import org.example.spout.RandomNumberSpout;

public class TopologyRunner {
    public static void main(String[] args) throws Exception {
        runTopology();
    }

    public static void runTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        IRichSpout random = new RandomNumberSpout();
        builder.setSpout("randomNumberSpout", random);

        IBasicBolt filtering = new FilteringBolt();
        builder.setBolt("filteringBolt", filtering)
                .shuffleGrouping("randomNumberSpout");

        IWindowedBolt aggregating = new AggregatingBolt()
                .withTimestampField("timestamp")
                .withLag(BaseWindowedBolt.Duration.seconds(1))
                .withWindow(BaseWindowedBolt.Duration.seconds(5));
        builder.setBolt("aggregatingBolt", aggregating)
                .shuffleGrouping("filteringBolt");

        String filePath = "./src/main/resources/data.txt";
        IRichBolt file = new FileWritingBolt(filePath);
        builder.setBolt("fileBolt", file)
                .shuffleGrouping("aggregatingBolt");

        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Test", config, builder.createTopology());
    }
}

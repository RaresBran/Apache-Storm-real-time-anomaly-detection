package org.example.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSensorDataSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        float humidity = 30 + (50 - 30) * random.nextFloat(); // Simulează umiditatea
        float temperature = 10 + (30 - 10) * random.nextFloat(); // Simulează temperatura
        float luminosity = 100 + (1000 - 100) * random.nextFloat(); // Simulează luminozitatea

        collector.emit(new Values(humidity, temperature, luminosity));
        Utils.sleep(1000); // Emit un nou tuple la fiecare secundă
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("humidity", "temperature", "luminosity"));
    }
}

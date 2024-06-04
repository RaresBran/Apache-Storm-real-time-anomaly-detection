package org.project.spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;

public class KafkaSpoutConfigBuilder {

    public KafkaSpoutConfigBuilder() { }

    public static KafkaSpoutConfig<String, String> createKafkaSpoutConfig(String bootstrapServers, String topic) {
        KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(
                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(1),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10),
                Integer.MAX_VALUE,
                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(1)
        );

        return KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-consumer-group")
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .setRetry(retryService)
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
                .build();
    }
}

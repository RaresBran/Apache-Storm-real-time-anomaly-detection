package org.project.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.project.utility.Thresholds;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisService {
    private final Jedis jedis;
    private final ObjectMapper objectMapper;
    private static final Thresholds DEFAULT_THRESHOLDS = new Thresholds(
            0.0, 10.0,
            0.0, 100.0,
            0.0, 1000.0,
            0.0, 5.0,
            -40.0, 50.0
    );

    public RedisService(String redisHost, int redisPort) {
        this.jedis = new Jedis(redisHost, redisPort);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public Thresholds getThresholds(String id) {
        Map<String, String> thresholdsMap = jedis.hgetAll(id);
        if (thresholdsMap.isEmpty()) {
            return DEFAULT_THRESHOLDS;
        }
        return objectMapper.convertValue(thresholdsMap, Thresholds.class);
    }

    public void close() {
        jedis.close();
    }
}

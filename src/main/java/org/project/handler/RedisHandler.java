package org.project.handler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.project.utility.Thresholds;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisHandler {
    private final Jedis jedis;
    private final ObjectMapper objectMapper;
    private static final Thresholds DEFAULT_THRESHOLDS = new Thresholds(
            100.0, 0.0,
            50.0, -40.0,
            10.0, 0.0,
            1000.0, 0.0,
            5.0, 0.0
    );
    private static final String EMAIL_LIST_KEY = "alert:emailList";

    public RedisHandler(String redisHost, int redisPort) {
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

    public List<String> getAlertEmailList() {
        Set<String> emails = jedis.smembers(EMAIL_LIST_KEY);
        return new ArrayList<>(emails);
    }

    public void close() {
        jedis.close();
    }
}

package org.project.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;

public class InfluxDBService {
    private final InfluxDBClient influxDBClient;

    public InfluxDBService(String influxDbUrl, String bucket, String org, String token) {
        this.influxDBClient = InfluxDBClientFactory.create(influxDbUrl, token.toCharArray(), org, bucket);
    }

    public void writePoint(Point point) {
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        writeApi.writePoint(point);
    }

    public void close() {
        if (influxDBClient != null) {
            influxDBClient.close();
        }
    }
}

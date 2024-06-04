package org.project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class AlertBolt extends BaseRichBolt {
    private static final double CO_THRESHOLD = 10.0;
    private static final double SMOKE_THRESHOLD = 5.0;
    private static final double TEMP_THRESHOLD = 50.0;
    private static final Logger log = LoggerFactory.getLogger(AlertBolt.class);
    private OutputCollector outputCollector;
    private transient Mailer mailer;

    private static class SensorWindow {
        LinkedList<Double> values = new LinkedList<>();
        long startTime;
    }

    private Map<String, SensorWindow> sensorWindows;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        mailer = MailerBuilder
                .withSMTPServer("sandbox.smtp.mailtrap.io", 2525, "d4d2a3a004e61a", "1670639032d692")
                .withTransportStrategy(TransportStrategy.SMTP_TLS)
                .withDebugLogging(false)
                .buildMailer();

        sensorWindows = new HashMap<>();
        sensorWindows.put("co", new SensorWindow());
        sensorWindows.put("smoke", new SensorWindow());
        sensorWindows.put("temp", new SensorWindow());
    }

    @Override
    public void execute(Tuple tuple) {
        long ts = tuple.getLongByField("ts");
        double co = tuple.getDoubleByField("co");
        double smoke = tuple.getDoubleByField("smoke");
        double temp = tuple.getDoubleByField("temp");

        updateSensorWindow(sensorWindows.get("co"), co, ts);
        updateSensorWindow(sensorWindows.get("smoke"), smoke, ts);
        updateSensorWindow(sensorWindows.get("temp"), temp, ts);

        double coMean = calculateMean(sensorWindows.get("co"));
        double smokeMean = calculateMean(sensorWindows.get("smoke"));
        double tempMean = calculateMean(sensorWindows.get("temp"));

        if (coMean > CO_THRESHOLD || smokeMean > SMOKE_THRESHOLD || tempMean > TEMP_THRESHOLD) {
            sendAlert(coMean, smokeMean, tempMean, ts);
            Values values = new Values(true, ts);
            outputCollector.emit(values);
        }
    }

    private void updateSensorWindow(SensorWindow window, double value, long ts) {
        window.values.addLast(value);
        window.startTime = ts;
        long currentTime = System.currentTimeMillis();

        // Remove values older than 5 seconds
        while (!window.values.isEmpty() && (currentTime - window.startTime) > 5000) {
            window.values.removeFirst();
        }
    }

    private double calculateMean(SensorWindow window) {
        return window.values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private void sendAlert(double coMean, double smokeMean, double tempMean, long timestamp) {
        String message = """
                Alert: Thresholds surpassed
                CO Mean: %.2f
                Smoke Mean: %.2f
                Temp Mean: %.2f
                Timestamp: %d
                
                :*
                """.formatted(coMean, smokeMean, tempMean, timestamp);

        Email email = EmailBuilder.startingBlank()
                .from("Sensor Alerts", "sensor@alert.com")
                .to("Admin", "raresbran@gmail.com")
                .withSubject("Alert from Storm Topology")
                .withPlainText(message)
                .buildEmail();

        log.info("Email alert sent");
        mailer.sendMail(email);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("isAlertSent", "timestamp"));
    }
}

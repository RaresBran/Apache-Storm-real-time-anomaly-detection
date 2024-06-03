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

import java.util.Map;

public class AlertBolt extends BaseRichBolt {
    private static final int ALERT_THRESHOLD = 400;
    private OutputCollector outputCollector;
    private transient Mailer mailer;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        mailer = MailerBuilder
                .withSMTPServer("smtp.gmail.com", 587, "yourgmail@gmail.com", "yourGmailPassword")
                .withTransportStrategy(TransportStrategy.SMTP_TLS)
                .withDebugLogging(true)
                .buildMailer();
    }

    @Override
    public void execute(Tuple tuple) {
        int sumOfOperations = tuple.getIntegerByField("sumOfOperations");
        if(sumOfOperations > 400 ) {
            long timestamp;
            timestamp = System.currentTimeMillis();
            Values values = new Values(true, timestamp);
            outputCollector.emit(values);

            String message = """
                    Alert: sumOfOperations larger than 400
                    Timestamp: %d
                    
                    :*
                    """
                    .formatted(timestamp);

            Email email = EmailBuilder.startingBlank()
                    .from("Your Name", "your@email.com")
                    .to("Recipient Name", "recipient@email.com")
                    .withSubject("Alert from Storm Topology")
                    .withPlainText(message)
                    .buildEmail();

            mailer.sendMail(email);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("isAlertSent", "timestamp"));
    }
}

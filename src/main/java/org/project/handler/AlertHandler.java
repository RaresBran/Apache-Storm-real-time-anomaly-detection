package org.project.handler;

import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AlertHandler {
    private static final Logger log = LoggerFactory.getLogger(AlertHandler.class);
    private final Mailer mailer;
    private final RedisHandler redisHandler;

    public AlertHandler(RedisHandler redisHandler) {
        this.mailer = MailerBuilder
                .withSMTPServer("sandbox.smtp.mailtrap.io", 2525, "d4d2a3a004e61a", "1670639032d692")
                .withTransportStrategy(TransportStrategy.SMTP_TLS)
                .withDebugLogging(false)
                .buildMailer();
        this.redisHandler = redisHandler;
    }

    public void sendEmailAlert(String subject, String message) {
        List<String> emailList = redisHandler.getAlertEmailList();

        Email email = EmailBuilder.startingBlank()
                .from("Sensor Alerts", "sensor@alert.com")
                .toMultiple(emailList)
                .withSubject(subject)
                .withPlainText(message)
                .buildEmail();

        log.info("Sending email alert to {}: {}", emailList, subject);
//        mailer.sendMail(email);
    }
}

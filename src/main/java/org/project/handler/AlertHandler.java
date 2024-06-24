package org.project.handler;

import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AlertHandler {
    private static final Logger log = LoggerFactory.getLogger(AlertHandler.class);
    private final Mailer mailer;
    private final RedisHandler redisHandler;
    private final ExecutorService executorService;

    public AlertHandler(RedisHandler redisHandler) {
        this.mailer = MailerBuilder
                .withSMTPServer("maildev", 1025)  // MailDev SMTP server
                .withTransportStrategy(TransportStrategy.SMTP)
                .withDebugLogging(false)
                .buildMailer();
        this.redisHandler = redisHandler;
        this.executorService = Executors.newCachedThreadPool(); // Cached thread pool
    }

    public void sendEmailAlert(String subject, String message) {
        List<String> emailList = redisHandler.getAlertEmailList();
        Email email = EmailBuilder.startingBlank()
                .from("Sensor Alerts", "sensor@alert.com")
                .toMultiple(emailList)
                .withSubject(subject)
                .withPlainText(message)
                .buildEmail();

        log.info("Queueing email alert to {}: {}", emailList, subject);
        executorService.submit(() -> {
            log.info("Sending email alert to {}: {}", emailList, subject);
            mailer.sendMail(email);
        });
    }

    public void shutdown() {
        executorService.shutdown();
    }
}

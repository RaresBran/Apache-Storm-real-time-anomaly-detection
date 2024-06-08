package org.project.service;

import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertService {
    private static final Logger log = LoggerFactory.getLogger(AlertService.class);
    private final Mailer mailer;

    public AlertService() {
        this.mailer = MailerBuilder
                .withSMTPServer("sandbox.smtp.mailtrap.io", 2525, "d4d2a3a004e61a", "1670639032d692")
                .withTransportStrategy(TransportStrategy.SMTP_TLS)
                .withDebugLogging(false)
                .buildMailer();
    }

    public void sendAlert(String subject, String message) {
        Email email = EmailBuilder.startingBlank()
                .from("Sensor Alerts", "sensor@alert.com")
                .to("Admin", "admin@example.com")
                .withSubject(subject)
                .withPlainText(message)
                .buildEmail();

        log.info("Sending email alert: {}", subject);
        mailer.sendMail(email);
    }
}

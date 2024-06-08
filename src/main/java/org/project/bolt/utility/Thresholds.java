package org.project.bolt.utility;

import java.io.Serializable;

public record Thresholds(double coLow, double coHigh, double humidityLow, double humidityHigh, double lpgLow,
                         double lpgHigh, double smokeLow, double smokeHigh, double tempLow,
                         double tempHigh) implements Serializable {
}

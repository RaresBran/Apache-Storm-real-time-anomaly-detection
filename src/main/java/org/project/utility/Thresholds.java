package org.project.utility;

public record Thresholds (
    double humidityUpper,
    double humidityLower,
    double temperatureUpper,
    double temperatureLower,
    double coUpper,
    double coLower,
    double lpgUpper,
    double lpgLower,
    double smokeUpper,
    double smokeLower
) {}


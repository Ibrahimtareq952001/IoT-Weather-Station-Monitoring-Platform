package com.weather.station.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Nested weather measurements within a WeatherReading.
 *
 * Jackson's @JsonProperty maps the Java field name to the JSON key name.
 * Without it, Jackson would serialize "windSpeed" (camelCase), but the
 * spec requires "wind_speed" (snake_case).
 */
public class WeatherData {

    @JsonProperty("humidity")
    private int humidity;       // percentage, 0–100

    @JsonProperty("temperature")
    private int temperature;    // degrees Fahrenheit

    @JsonProperty("wind_speed")
    private int windSpeed;      // km/h

    public WeatherData(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    // Jackson requires a no-arg constructor for deserialization
    public WeatherData() {}

    public int getHumidity()    { return humidity; }
    public int getTemperature() { return temperature; }
    public int getWindSpeed()   { return windSpeed; }

    public void setHumidity(int humidity)       { this.humidity = humidity; }
    public void setTemperature(int temperature) { this.temperature = temperature; }
    public void setWindSpeed(int windSpeed)      { this.windSpeed = windSpeed; }
}

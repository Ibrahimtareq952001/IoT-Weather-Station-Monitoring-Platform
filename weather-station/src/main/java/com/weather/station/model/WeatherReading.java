package com.weather.station.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Complete weather status message as defined in the system spec.
 *
 * JSON shape:
 * {
 *   "station_id": 1,
 *   "s_no": 42,
 *   "battery_status": "medium",
 *   "status_timestamp": 1681521224,
 *   "weather": { "humidity": 35, "temperature": 100, "wind_speed": 13 }
 * }
 */
public class WeatherReading {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sequenceNumber;

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    @JsonProperty("weather")
    private WeatherData weather;

    public WeatherReading(long stationId, long sequenceNumber,
                          String batteryStatus, long statusTimestamp,
                          WeatherData weather) {
        this.stationId = stationId;
        this.sequenceNumber = sequenceNumber;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public WeatherReading() {}

    public long getStationId()       { return stationId; }
    public long getSequenceNumber()  { return sequenceNumber; }
    public String getBatteryStatus() { return batteryStatus; }
    public long getStatusTimestamp() { return statusTimestamp; }
    public WeatherData getWeather()  { return weather; }

    public void setStationId(long stationId)           { this.stationId = stationId; }
    public void setSequenceNumber(long sequenceNumber) { this.sequenceNumber = sequenceNumber; }
    public void setBatteryStatus(String batteryStatus) { this.batteryStatus = batteryStatus; }
    public void setStatusTimestamp(long statusTimestamp){ this.statusTimestamp = statusTimestamp; }
    public void setWeather(WeatherData weather)        { this.weather = weather; }
}

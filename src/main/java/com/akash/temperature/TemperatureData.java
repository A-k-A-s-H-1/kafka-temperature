package com.akash.temperature;


import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class TemperatureData {

    @Id
    private String city;
    private double temperature;

    protected TemperatureData() {}

    public TemperatureData(String city, double temperature) {
        this.city = city;
        this.temperature = temperature;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
}


package com.healthmonitor.healthmonitorbackend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.annotations.SerializedName;

public class Message {
    @SerializedName("serviceName")
    String serviceName;
    @SerializedName("Timestamp")
    Long Timestamp;
    @SerializedName("CPU")
    Double CPU;
    @SerializedName("RAM")
    Ram RAM;
    @SerializedName("Disk")
    Disk Disk;


    public Message(String serviceName, Long Timestamp, Double CPU, Ram RAM, Disk Disk) {
        this.serviceName = serviceName;
        this.Timestamp = Timestamp;
        this.CPU = CPU;
        this.RAM = RAM;
        this.Disk = Disk;
    }

    @JsonProperty("serviceName")
    public String getServiceName() {
        return serviceName;
    }

    @JsonProperty("Timestamp")
    public Long getTimeStamp() {
        return Timestamp;
    }

    @JsonProperty("CPU")
    public Double getCpu() {
        return CPU;
    }

    @JsonProperty("RAM")
    public Ram getRam() {
        return RAM;
    }

    public String toJsonString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(this);
            return json;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    @JsonProperty("Disk")
    public Disk getDisk() {
        return Disk;
    }
}

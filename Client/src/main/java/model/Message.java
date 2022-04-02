package model;

import com.google.gson.annotations.SerializedName;

public class Message {
    @SerializedName("serviceName")
    String serviceName;
    @SerializedName("Timestamp")
    Long timeStamp;
    @SerializedName("CPU")
    Double cpu;
    @SerializedName("RAM")
    Ram ram;

    public Message(String serviceName, Long timeStamp, Double cpu, Ram ram) {
        this.serviceName = serviceName;
        this.timeStamp = timeStamp;
        this.cpu = cpu;
        this.ram = ram;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public Double getCpu() {
        return cpu;
    }

    public Ram getRam() {
        return ram;
    }
}

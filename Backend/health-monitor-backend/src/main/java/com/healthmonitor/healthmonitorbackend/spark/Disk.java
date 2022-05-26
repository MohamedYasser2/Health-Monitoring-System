package com.healthmonitor.healthmonitorbackend.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import net.minidev.json.JSONValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class Disk {
    @SerializedName("Total")
    Double total;
    @SerializedName("Free")
    Double free;

    public Disk(Double total, Double free) {
        this.total = total;
        this.free = free;
    }


    public String toJsonString() {
        Map jsonObject=new LinkedHashMap();
        jsonObject.put("Total", this.total);
        jsonObject.put("Free", this.free);
        String jsonText = JSONValue.toJSONString(jsonObject);
        return jsonText;
    }

    @JsonProperty("Total")
    public Double getTotal() {
        return total;
    }

    @JsonProperty("Free")
    public Double getFree() {
        return free;
    }
}

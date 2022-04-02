package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import org.json.simple.JSONValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class Ram {
    @JsonProperty("Total")
    public Double getTotal() {
        return total;
    }

    @JsonProperty("Free")
    public double getFree() {
        return free;
    }

    @SerializedName("Total")
    Double total;
    @SerializedName("Free")
    double free;

    public Ram(Double total, Double free) {
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

}

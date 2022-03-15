package model;

import org.json.simple.JSONValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class Ram {
    int total;
    double free;

    public Ram(int total, double free) {
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

package model;

import org.json.simple.JSONValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class Disk {
    int total;
    int free;

    public Disk(int total, int free) {
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

import com.google.gson.annotations.SerializedName;
import net.minidev.json.JSONValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class Ram {
    @SerializedName("Total")
    Double total;
    @SerializedName("Free")
    Double free;

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

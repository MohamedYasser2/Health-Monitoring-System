package health_message;

import com.google.gson.Gson;
import model.Disk;
import model.Ram;
import model.ServiceName;
import org.json.simple.JSONValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;


public class HealthMessageGenerator implements IHealthMessageGenerator{
    @Override
    public String generateMessage() {
        Map jsonObject=new LinkedHashMap();
        jsonObject.put("serviceName", ServiceName.values()[new Random().nextInt(ServiceName.values().length)].toString());
        jsonObject.put("Timestamp", generateTimeStamp());
        jsonObject.put("CPU", generateCPU());
        jsonObject.put("RAM", generateRam());
        jsonObject.put("Disk", generateDisk());
        String jsonText = JSONValue.toJSONString(jsonObject);
        return jsonText;
    }

    @Override
    public Double generateCPU() {
        Double random = Math.random();
        Double cpu = Math.round(random * 100.0) / 100.0;
        return cpu;
    }

    @Override
    public Long generateTimeStamp() {
        Long generatedInteger = (long) Math.floor(Math.random() * 9000_000_000L) + 1_000_000_000L;
        return generatedInteger;
    }

    @Override
    public String generateRam() {
        Random rand = new Random();
        int total = rand.nextInt(64);
        float free = rand.nextFloat() * (64) + 0;
        Ram ram = new Ram(total, free);
        return ram.toJsonString();
    }

    @Override
    public String generateDisk() {
        Random rand = new Random();
        int total = rand.nextInt(1000);
        int free = rand.nextInt(1000);
        Disk disk = new Disk(total, free);
        return disk.toJsonString();
    }

}

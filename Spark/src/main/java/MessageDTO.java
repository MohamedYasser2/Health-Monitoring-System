import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.annotations.SerializedName;

public class MessageDTO {

    public MessageDTO(String serviceName, Long Timestamp, Double CPU, Double RAM, Double Disk) {
        this.serviceName = serviceName;
        this.Timestamp = Timestamp;
        this.CPU = CPU;
        this.RAM = RAM;
        this.Disk = Disk;
    }

    @SerializedName("serviceName")
    String serviceName;
    @SerializedName("Timestamp")
    Long Timestamp;
    @SerializedName("CPU")
    Double CPU;
    @SerializedName("RAM")
    Double RAM;
    @SerializedName("Disk")
    Double Disk;


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
    public Double getDisk() {
        return Disk;
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
    public Double getRam() {
        return RAM;
    }

}

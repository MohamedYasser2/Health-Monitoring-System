package health_message;

public interface IHealthMessageGenerator {
    String generateMessage();
    Double generateCPU();
    Long generateTimeStamp();
    String generateRam();
    String generateDisk();

}

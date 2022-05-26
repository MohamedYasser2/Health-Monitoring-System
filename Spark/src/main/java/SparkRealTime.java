import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SparkRealTime {
    public static void main(String[] args) throws Exception {
        SparkThread sparkThread = new SparkThread();
        sparkThread.start();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Scheduler(), 1, 1, TimeUnit.MINUTES);
    }
}
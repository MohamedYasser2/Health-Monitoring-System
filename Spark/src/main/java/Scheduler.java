import hadoop.MapReduce;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Scheduler implements Runnable{
    @Override
    public void run() {
        System.out.println("hellllloooooz");
        try {
            MapReduce.runJob();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        File folder = new File("/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Spark/Parquet/");
            for (File f : folder.listFiles()) {
                if (f.getName().endsWith(".parquet") || f.getName().endsWith(".parquet.crc")) {
                    f.delete(); // may fail mysteriously - returns boolean you may want to check
                }
            }
            folder = new File("/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Spark/json/");
            for (File f : folder.listFiles()) {
                if (f.getName().endsWith(".json")) {
                    f.delete(); // may fail mysteriously - returns boolean you may want to check
                }
            }
        System.out.println("deleted");
    }
}

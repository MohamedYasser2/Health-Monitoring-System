import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SparkThread extends Thread {

    public void run(){
        super.run();
        while (true) {
            //Your Code
            try {
                Logger.getLogger("org.apache").setLevel(Level.WARN);

                SparkSession spark = SparkSession
                        .builder()
                        .appName("Application Name")
                        .config("spark.master", "local")
                        .getOrCreate();

                StructType schema = new StructType().add("serviceName", DataTypes.StringType)
                        .add("RAM", DataTypes.DoubleType)
                        .add("CPU", DataTypes.DoubleType)
                        .add("Stamp", DataTypes.LongType)
                        .add("Disk", DataTypes.DoubleType);


                Dataset<Row> ds = spark.readStream().schema(schema).format("json")
                        .json("/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Spark/json");

                StreamingQuery query = ds.writeStream()
                        .format("parquet")
                        .option("path", "/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Spark/Parquet")
                        .option("checkpointLocation", "/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Spark/checkpoint")
                        .start();

                    query.awaitTermination();

                ds.show();
            }
            catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            catch (StreamingQueryException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

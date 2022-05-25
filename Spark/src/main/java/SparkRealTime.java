import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkRealTime {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .appName("Application Name")
                .config("spark.master", "local")
                .getOrCreate();

//        StructType schema2 = new StructType()
//                .add("Total", DataTypes.DoubleType)
//                .add("Free", DataTypes.DoubleType);
//;       StructType schema3 = new StructType()
//                .add("Total", DataTypes.DoubleType)
//                .add("Free", DataTypes.DoubleType);

        StructType schema = new StructType().add("serviceName", DataTypes.StringType)
                .add("RAM", DataTypes.DoubleType)
                .add("CPU", DataTypes.DoubleType)
                .add("Stamp", DataTypes.LongType)
                .add("Disk", DataTypes.DoubleType);

//        StructType schema = DataTypes.createStructType(new StructField[] {
//                DataTypes.createStructField("CPU",  DataTypes.StringType, true),
//                DataTypes.createStructField("Disk", DataTypes.StringType, true),
//                DataTypes.createStructField("RAM", DataTypes.StringType, true),
//                DataTypes.createStructField("Timestamp", DataTypes.StringType, true),
//                DataTypes.createStructField("serviceName", DataTypes.StringType, true)
//        });

        Dataset<Row> ds = spark.readStream().schema(schema).format("json")
                .json("/home/mohamed-yasser/Projects/Spark/json");

//        StreamingQuery query = ds.writeStream()
//                .format("console")
//                .start();
        StreamingQuery query=ds.writeStream()
                .format("parquet")
                .option("path", "/home/mohamed-yasser/Projects/Spark/Parquet")
                .option("checkpointLocation", "/home/mohamed-yasser/Projects/Spark/checkpoint")
                .start();

        query.awaitTermination();
        ds.show();
    }
}
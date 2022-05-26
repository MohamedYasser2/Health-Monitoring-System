package com.healthmonitor.healthmonitorbackend.spark;

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

        StructType schema = new StructType().add("serviceName", DataTypes.StringType)
                .add("RAM", DataTypes.DoubleType)
                .add("CPU", DataTypes.DoubleType)
                .add("Stamp", DataTypes.LongType)
                .add("Disk", DataTypes.DoubleType);



        Dataset<Row> ds = spark.readStream().schema(schema).format("json")
                .json("/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Backend/health-monitor-backend/json");

        StreamingQuery query=ds.writeStream()
                .format("parquet")
                .option("path", "/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Backend/health-monitor-backend/Parquet")
                .option("checkpointLocation", "/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Backend/health-monitor-backend/checkpoint")
                .start();

        query.awaitTermination();
        ds.show();
    }
}
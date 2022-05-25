package com.healthmonitor.healthmonitorbackend;


import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

// The driver program for mapreduce job.
public class AlphaCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {


        // Create job
        Job job = Job.getInstance(new Configuration(), "Utilization");

        job.setMapperClass(AlphaMapper.class);
        job.setReducerClass(AlphaReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-master:9000/inputs/"));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-master:9000/outputs/"));
//        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
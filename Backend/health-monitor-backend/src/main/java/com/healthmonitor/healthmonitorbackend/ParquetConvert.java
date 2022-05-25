package com.healthmonitor.healthmonitorbackend;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

public class ParquetConvert extends Configured implements Tool{

    /// Schema
    private static final Schema MAPPING_SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "    \"type\":    \"record\",\n" +
                    "    \"name\":    \"TextFile\",\n" +
                    "    \"doc\":    \"Text File\",\n" +
                    "    \"fields\":\n" +
                    "    [\n" +
                    "            {\"name\":    \"line\", \"type\":    \"string\"}\n"+
                    "    ]\n"+
                    "}\n");

    // Map function
    public static class ParquetConvertMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

        private GenericRecord record = new GenericData.Record(MAPPING_SCHEMA);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            record.put("line", value.toString());
            context.write(null, record);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "ParquetConvert");
        job.setJarByClass(getClass());
        job.setMapperClass(ParquetConvertMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        // setting schema
        AvroParquetOutputFormat.setSchema(job, MAPPING_SCHEMA);
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-master:9000/outputs/"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-master:9000/outputs1/"));
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception{
        int exitFlag = ToolRunner.run(new ParquetConvert(), args);
        System.exit(exitFlag);
    }
}
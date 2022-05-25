package com.healthmonitor.healthmonitorbackend;




import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduce {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, ArrayWritable> {
        //Long startDate;
        //Long endDate;
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            //   startDate = job.getLong("startDate", -1);
            // nb: last arg is the default value if option is not set
            //   endDate = job.getLong("endDate", -1);
            //   System.out.println(startDate);
            //   System.out.println(endDate);
        }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            System.out.println(value);
            String[] messageFields = value.toString().split(",");
            String timeStamp = messageFields[0];
            Long time = Long.parseLong(timeStamp);
            Date date = new Date(time);
            Format format = new SimpleDateFormat("yyyy MM dd HH:mm");
            String dateKey = format.format(date);
            System.out.println("Mapping");
            System.out.println(dateKey + "," + messageFields[1]);
            String[] strings = new String[]{
                    messageFields[2], messageFields[5], messageFields[3], messageFields[0]};
            output.collect(new Text(dateKey + "," + messageFields[1]), new TextArrayWritable(strings));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

        @Override
        public void reduce(Text key, Iterator<TextArrayWritable> values, OutputCollector<Text, TextArrayWritable> output, Reporter reporter) throws IOException {
            Double cpuSum = Double.valueOf(0);
            Double diskSum = Double.valueOf(0);
            Double ramSum = Double.valueOf(0);
            Double serviceSum = Double.valueOf(0);
            Long time = 0L;
            TextArrayWritable tmp;
            while (values.hasNext()) {
                tmp = values.next();
                String[] strings = tmp.toStrings();
                cpuSum += (Double.parseDouble(strings[0]));
                diskSum += (Double.parseDouble(strings[1]));
                ramSum += (Double.parseDouble(strings[2]));
                serviceSum++;
                time = Math.max(Long.parseLong(strings[3]), time);
            }
            cpuSum = cpuSum / serviceSum;
            diskSum = diskSum / serviceSum;
            ramSum = ramSum / serviceSum;
            String[] strings = new String[]{cpuSum.toString(), diskSum.toString(), ramSum.toString(), time.toString(), serviceSum.toString()};
            output.collect(key, new TextArrayWritable(strings));
        }
    }

    public static int runJob(Long startDate, Long endDate) throws IOException, InterruptedException, ClassNotFoundException {
//        Job job = Job.getInstance();
//
//        job.setJobName("Utilization");
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setMapperClass(Map.class);
//        job.setNumReduceTasks(0);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop-master:9000/inputs/"));
//        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop-master:9000/outputs/"));
//
//        job.waitForCompletion(true);
//        return 0;
        JobConf conf = new JobConf(Message.class);
        conf.setJobName("Utilization");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(TextArrayWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        Job job = Job.getInstance();

        // Set up Hadoop Output Format
//        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(new AvroParquetOutputFormat(), job);


//        AvroParquetOutputFormat.setSchema(job, );
//        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
//        ParquetOutputFormat.setEnableDictionary(job, true);


        conf.setOutputFormat(TextOutputFormat.class);
        System.out.println(startDate);
        System.out.println(endDate);
        conf.set("startDate", startDate.toString());
        conf.set("endDate", endDate.toString());
//        FileInputFormat.setInputPaths(conf, new Path("hdfs://hadoop-master:9000/inputs/"));
//        FileOutputFormat.setOutputPath(conf, new Path("hdfs://hadoop-master:9000/output/"));
        JobClient.runJob(conf);
        return 0;
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }
        Text[] texts;
        String[] stringNums;
        public TextArrayWritable(String[] strings) {
            super(Text.class);
            texts = new Text[strings.length];
            stringNums = strings;
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
}
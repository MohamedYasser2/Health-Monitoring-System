package com.healthmonitor.healthmonitorbackend;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.mapred.*;

public class MapReduce{
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,ArrayWritable> {
        Long startDate;
        Long endDate;
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            startDate = job.getLong("startDate", -1);
            // nb: last arg is the default value if option is not set
            endDate = job.getLong("endDate", -1);
            System.out.println(startDate);
            System.out.println(endDate);
        }


        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            Type listType = new TypeToken<List<Message>>() {}.getType();
            List<Message> messages = new Gson().fromJson(value.toString(), listType);

            for (Message message : messages) {
                String dateForm = startDate.toString();
                String messageDateForm = message.getTimeStamp().toString();
                if (dateForm.length() > messageDateForm.length()) {
                    for (int i = 0; i < dateForm.length() - messageDateForm.length() + 2; i++) {
                        messageDateForm += '0';
                    }
                }
                Long messageTime = Long.parseLong(messageDateForm);

                System.out.println("Start Date");
                System.out.println(startDate);
                System.out.println("End Date");
                System.out.println(endDate);
                System.out.println("Message Date");
                System.out.println(messageTime);
                if (messageTime > startDate && messageTime < endDate) {
                    System.out.println("Mapping");
                    String[] strings = new String[]{
                            message.getCpu().toString(), message.getDisk().getTotal().toString(), message.getRam().getTotal().toString(), message.getTimeStamp().toString()};
                    output.collect(new Text(message.getServiceName()), new TextArrayWritable(strings));
                }
            }
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
                cpuSum +=  (Double.parseDouble(strings[0]));
                diskSum += (Double.parseDouble(strings[1]));
                ramSum += (Double.parseDouble(strings[2]));
                serviceSum++;
                time = Math.max(Long.parseLong(strings[3]), time);
            }
            cpuSum = cpuSum/serviceSum;
            diskSum = diskSum/serviceSum;
            ramSum = ramSum/serviceSum;
            String[] strings = new String[] {cpuSum.toString(), diskSum.toString(), ramSum.toString(), time.toString(), serviceSum.toString()};
            output.collect(key, new TextArrayWritable(strings));
        }
    }

    public static void runJob(Long startDate, Long endDate) throws IOException {
        JobConf conf = new JobConf(Message.class);
        conf.setJobName("Utilization");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(TextArrayWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        System.out.println(startDate);
        System.out.println(endDate);
        conf.set("startDate", startDate.toString());
        conf.set("endDate", endDate.toString());
        FileInputFormat.setInputPaths(conf, new Path("hdfs://hadoop-master:9000/input/"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://hadoop-master:9000/output/"));
        JobClient.runJob(conf);
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
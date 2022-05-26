package hadoop;


import java.io.IOException;
import java.net.URI;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class MapReduce {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
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

            output.collect(new Text(dateKey + "," + messageFields[1]), new Text(messageFields[2] + "\t" + messageFields[5] + "\t" + messageFields[3] + "\t"  + messageFields[0]));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            Double cpuSum = Double.valueOf(0);
            Double diskSum = Double.valueOf(0);
            Double ramSum = Double.valueOf(0);
            Double serviceSum = Double.valueOf(0);
            Long time = 0L;
            while (values.hasNext()) {
                String s = values.next().toString();
                String[] strings = s.split("\t");
                cpuSum += (Double.parseDouble(strings[0]));
                diskSum += (Double.parseDouble(strings[1]));
                ramSum += (Double.parseDouble(strings[2]));
                serviceSum++;
                time = Math.max(Long.parseLong(strings[3]), time);
            }
            cpuSum = cpuSum / serviceSum;
            diskSum = diskSum / serviceSum;
            ramSum = ramSum / serviceSum;
            System.out.println("Reducing Key");
            System.out.println(key.toString());
            System.out.println(cpuSum + "\t" + diskSum + "\t" + ramSum + "\t" + time + "\t" + serviceSum + "\n");
            output.collect(key, new Text(cpuSum + "\t" + diskSum + "\t" + ramSum + "\t" + time + "\t" + serviceSum));
        }
    }

    public static int runJob() throws Exception {

        Path output = new Path("/outputs");
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://hadoop-master:9000"),config);

        // delete existing directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        output = new Path("/outputs");
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        JobConf conf = new JobConf(Message.class);
        conf.setJobName("Utilization");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path("hdfs://hadoop-master:9000/inputs/"));
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://hadoop-master:9000/outputs/"));
        JobClient.runJob(conf);

        ParquetConvert parquetConvert = new ParquetConvert();
        String[] args = new String[0];
        parquetConvert.run(args);
        output = new Path("/outputs");
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        return 0;
    }
}
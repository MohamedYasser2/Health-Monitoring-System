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

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            Type listType = new TypeToken<List<Message>>() {}.getType();
            List<Message> messages = new Gson().fromJson(value.toString(), listType);
            for (Message message : messages) {
                System.out.println("mapping");
                String[] strings = new String[] {
                        message.getCpu().toString(), message.getDisk().getTotal().toString(), message.getRam().getTotal().toString(), message.getTimeStamp().toString() };
                output.collect(new Text(message.serviceName), new TextArrayWritable(strings));
            }
            System.out.println("finisehd mapping");
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

        @Override
        public void reduce(Text key, Iterator<TextArrayWritable> values, OutputCollector<Text, TextArrayWritable> output, Reporter reporter) throws IOException {
            System.out.println("Reducing");
            System.out.println(values.hasNext());
            Double cpuSum = Double.valueOf(0);
            Double diskSum = Double.valueOf(0);
            Double ramSum = Double.valueOf(0);
            Double serviceSum = Double.valueOf(0);
            Long time = null;
            TextArrayWritable tmp;
            while (values.hasNext()) {
                tmp = values.next();
                String[] strings = tmp.toStrings();
                System.out.println(strings.length);
                System.out.println("before parsing");
                System.out.println(strings.length);
                cpuSum +=  (Double.parseDouble(strings[0]));
                diskSum += (Double.parseDouble(strings[1]));
                ramSum += (Double.parseDouble(strings[2]));
                serviceSum++;
                System.out.println("After Parsing");
            }
            cpuSum = cpuSum/serviceSum;
            diskSum = diskSum/serviceSum;
            ramSum = ramSum/serviceSum;
            String[] strings = new String[] {cpuSum.toString(), diskSum.toString(), ramSum.toString()};
            System.out.println("After Parsing Writables");
            output.collect(key, new TextArrayWritable(strings));
            System.out.println("finished");
        }
    }

        public static void main(String[] args) throws Exception {
            JobConf conf = new JobConf(Message.class);
            conf.setJobName("Utilization");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(TextArrayWritable.class);
            conf.setMapperClass(Map.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path("hdfs://hadoop-master:9000/"));
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
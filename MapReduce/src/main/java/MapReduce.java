import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class MapReduce{
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,ArrayWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            System.out.println(value);
            Type listType = new TypeToken<List<Message>>() {}.getType();
            List<Message> messages = new Gson().fromJson(value.toString(), listType);
            for (Message message : messages) {
                Writable[] writables = new Writable[]{new DoubleWritable(message.getCpu()), new DoubleWritable(message.getDisk().getTotal()), new DoubleWritable(message.getRam().getTotal()), new LongWritable(message.getTimeStamp())};
                output.collect(new Text(message.serviceName), new ArrayWritable(ArrayWritable.class, writables));

            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, ArrayWritable, Text, ArrayWritable> {

        @Override
        public void reduce(Text key, Iterator<ArrayWritable> values, OutputCollector<Text, ArrayWritable> output, Reporter reporter) throws IOException {
            System.out.println("Reducing");
            double cpuSum = 0;
            double diskSum = 0;
            double ramSum = 0;
            double serviceSum = 0;
            Long time = null;
            while (values.hasNext()) {
                System.out.println(values);
//                cpuSum +=  values.next().getCpu();
//                diskSum += values.next().getDisk();
//                ramSum += values.next().getRam();
//                serviceSum++;
//            }
//            cpuSum = cpuSum/serviceSum;
//            diskSum = diskSum/serviceSum;
//            ramSum = ramSum/serviceSum;
//            values v = new values(cpuSum,diskSum,ramSum,time,serviceSum);
//            output.collect(key, v);
            }
        }

        public static void main(String[] args) throws Exception {
            JobConf conf = new JobConf(Message.class);
            conf.setJobName("Utilization");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(ArrayWritable.class);
            conf.setMapperClass(Map.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path("hdfs://hadoop-master:9000/"));
            FileOutputFormat.setOutputPath(conf, new Path("hdfs://hadoop-master:9000/output/"));
            JobClient.runJob(conf);
//        final String HDFS_ROOT_URL = "hdfs://hadoop-master:9000";
//        Configuration conf = new Configuration();
//        FileSystem.get(URI.create(HDFS_ROOT_URL),conf);
//        Job job = Job.getInstance(conf,"File Utilization Program");
//
//        job.setJarByClass(MapReduce.class);
//        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
////        Path outputPath = new Path("/output.log");
////        outputPath.getFileSystem(conf).delete(outputPath);
//
//        //Configuring the input/output path from the filesystem into the job
//        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-master:9000/"));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-master:9000/output/"));
//
//        boolean success = job.waitForCompletion(true);
//        System.out.println(success);
//        //exiting the job only if the flag value becomes false
//        System.exit(success ? 0 : 1);
        }
    }
}
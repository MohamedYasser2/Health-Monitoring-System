import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class MapReduce{
    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
        public void map(LongWritable key, Message value,Context context) throws IOException,InterruptedException{
            values val = new values(value.getCpu(),value.getDisk().total,value.getRam().total,value.getTimeStamp());
            context.write(new Text(value.serviceName),val);
//            String line = value.toString();
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            while (tokenizer.hasMoreTokens()) {
//                value.set(tokenizer.nextToken());
//                context.write(value, new IntWritable(1))
//            }
        }
    }
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<values> values,Context context) throws IOException,InterruptedException {
            double cpuSum=0;double diskSum=0;double ramSum=0;
            double serviceSum=0;Long time = null;
            for(values x: values) {
                cpuSum += x.getCpu();
                diskSum +=x.getDisk();
                ramSum +=x.getRam();
                serviceSum++;
            }
            cpuSum = cpuSum/serviceSum;
            diskSum = diskSum/serviceSum;
            ramSum = ramSum/serviceSum;
            values v = new values(cpuSum,diskSum,ramSum,time,serviceSum);
            context.write(key, v);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        Job job = new Job(conf,"File Utilization Program");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
        //Configuring the input/output path from the filesystem into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //deleting the output path automatically from hdfs so that we don't have to delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
package com.healthmonitor.healthmonitorbackend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class AlphaReducer extends Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Text values, Context context)
            throws IOException, InterruptedException {
        Double cpuSum = Double.valueOf(0);
        Double diskSum = Double.valueOf(0);
        Double ramSum = Double.valueOf(0);
        Double serviceSum = Double.valueOf(0);
        Long time = 0L;
        MapReduce.TextArrayWritable tmp;
        String s = values.toString();

//        while (values.hasNext()) {
//            tmp = values.next();
//            String[] strings = tmp.toStrings();
        String[] strings = values.toString().split("\t");
            cpuSum += (Double.parseDouble(strings[0]));
            diskSum += (Double.parseDouble(strings[1]));
            ramSum += (Double.parseDouble(strings[2]));
            serviceSum++;
            time = Math.max(Long.parseLong(strings[3]), time);
//        }
        cpuSum = cpuSum / serviceSum;
        diskSum = diskSum / serviceSum;
        ramSum = ramSum / serviceSum;
//        String[] strings = new String[]{cpuSum.toString(), diskSum.toString(), ramSum.toString(), time.toString(), serviceSum.toString()};
        context.write(key, new Text(cpuSum + "\t" + diskSum + "\t" + ramSum + "\t" +  time + "\t" + serviceSum + "\n"));
    }
}

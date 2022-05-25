package com.healthmonitor.healthmonitorbackend;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;


// A mapper class converting each line of input into a key/value pair
// Each character is turned to a key with value as 1
public class AlphaMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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

//        context.write(new Text(dateKey + "," + messageFields[1]), new MapReduce.TextArrayWritable(strings));
        context.write(new Text(dateKey + "," + messageFields[1]), new Text(messageFields[2] + "\t" + messageFields[5] + "\t" + messageFields[3] + "\t"  + messageFields[0]));

    }
}

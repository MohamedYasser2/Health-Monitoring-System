package com.healthmonitor.healthmonitorbackend;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException{

        }
}

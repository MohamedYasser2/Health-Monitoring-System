//package com.healthmonitor.healthmonitorbackend;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//
//import java.io.IOException;
//import java.net.URI;
//import java.util.Timer;
//import java.util.TimerTask;
//
//
//public class Scheduler{
//    public void givenUsingTimer_whenSchedulingDailyTask_thenCorrect() {
//        TimerTask repeatedTask = new TimerTask() {
//            public void run() {
//                String[] args = new String[0];
//                Path output = new Path("/outputs");
//                Configuration conf = new Configuration();
//                FileSystem hdfs = null;
//                try {
//                    hdfs = FileSystem.get(URI.create("hdfs://hadoop-master:9000"),conf);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//
//                // delete existing directory
//                try {
//                    if (hdfs.exists(output)) {
//                        hdfs.delete(output, true);
//                    }
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                output = new Path("/outputs1");
//                try {
//                    if (hdfs.exists(output)) {
//                        try {
//                            hdfs.delete(output, true);
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//                    }
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                try {
//                    MapReduce.runJob();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                } catch (ClassNotFoundException e) {
//                    throw new RuntimeException(e);
//                }
//                ParquetConvert parquetConvert = new ParquetConvert();
//                try {
//                    parquetConvert.run(args);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//                output = new Path("/outputs");
//                try {
//                    if (hdfs.exists(output)) {
//                        hdfs.delete(output, true);
//                    }
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        };
//        Timer timer = new Timer("Timer");
//
//        long delay = 1000L;
//        long period = 1000L * 60L * 60L * 24L;
////        long period = 1000L * 60L;
//        timer.scheduleAtFixedRate(repeatedTask, delay, period);
//    }
//}

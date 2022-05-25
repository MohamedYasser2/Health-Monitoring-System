package com.healthmonitor.healthmonitorbackend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.healthmonitor.healthmonitorbackend.MapReduce.runJob;
import static java.lang.Character.isDigit;
import static java.lang.System.out;


@RestController
@RequestMapping("/api")
@CrossOrigin
public class HomeController {
    @GetMapping("/getstatistics")
    public ArrayList<String> getStatistics(@RequestParam String startDate, @RequestParam String endDate) throws Exception {
        ArrayList<String> test = new ArrayList<>();
        test.add(startDate);
        test.add(endDate);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        long fromDateTime = 0;
        long toDateTime = 0;
        try {
            Date fromDate = f.parse(startDate);
            fromDateTime = fromDate.getTime();
            out.println(fromDateTime);
            Date toDate = f.parse(endDate);
            toDateTime = toDate.getTime();
            out.println(toDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long Totalstart = System.nanoTime();
        ArrayList<String> answer = new DuckDBManager().queryBatchView(fromDateTime, toDateTime);
//        String[] args = new String[0];
//        Path output = new Path("/outputs");
//        Configuration conf = new Configuration();
//        FileSystem hdfs = FileSystem.get(URI.create("hdfs://hadoop-master:9000"),conf);
//
//        // delete existing directory
//        if (hdfs.exists(output)) {
//            hdfs.delete(output, true);
//        }
//        output = new Path("/outputs1");
//        if (hdfs.exists(output)) {
//            hdfs.delete(output, true);
//        }
//        MapReduce.runJob();
//        ParquetConvert parquetConvert = new ParquetConvert();
//        parquetConvert.run(args);
//        output = new Path("/outputs");
//        if (hdfs.exists(output)) {
//            hdfs.delete(output, true);
//        }
//
////        runJob();
//        long finish = System.nanoTime();
//        long timeElapsed = finish - Totalstart;
//        double elapsedTimeInSecond = (double) timeElapsed / 1_000_000_000;
//        out.println("Time is  " + elapsedTimeInSecond + " seconds");
//        out.println("Time is  " + elapsedTimeInSecond / 60 + " Minutes");
//        out.println("Throuput is " + (10000 * 215) / elapsedTimeInSecond + " records/second");
        return answer;
    }
}
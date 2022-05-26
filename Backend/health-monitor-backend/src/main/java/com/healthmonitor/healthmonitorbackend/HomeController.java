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
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        out.println(startDate);
        out.println(endDate);
        long fromDateTime = 0;
        long toDateTime = 0;
        try {
            Date fromDate = inputFormat.parse(startDate);
            fromDateTime = fromDate.getTime();
            out.println(fromDateTime);
            Date toDate = inputFormat.parse(endDate);
            toDateTime = toDate.getTime();
            out.println(toDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        ArrayList<String> answer = new DuckDBManager().combineTwoQueries(new DuckDBManager().queryBatchView(fromDateTime, toDateTime), new DuckDBManager().queryRealtimeView(fromDateTime, toDateTime));
        return answer;
    }
}
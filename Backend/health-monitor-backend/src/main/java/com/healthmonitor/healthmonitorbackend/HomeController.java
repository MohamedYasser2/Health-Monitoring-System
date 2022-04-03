package com.healthmonitor.healthmonitorbackend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static com.healthmonitor.healthmonitorbackend.MapReduce.runJob;
import static java.lang.Character.isDigit;


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
            fromDateTime = fromDate. getTime();
            System.out.println(fromDateTime);
            Date toDate = f.parse(endDate);
            toDateTime = toDate. getTime();
            System.out.println(toDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        runJob(fromDateTime, toDateTime);
        HDFSDemo demo = new HDFSDemo();
        String path = "/output/part-00000";
        String content = demo.printHDFSFileContents(path);
        System.out.println(parse(content));
        return parse(content);
    }

    public class HDFSDemo {
        public static final String HDFS_ROOT_URL = "hdfs://hadoop-master:9000";
        private Configuration conf;

        public HDFSDemo() {
            conf = new Configuration();
        }

        // Example - Print hdfs file contents to console using Java
        public String printHDFSFileContents(String filePath) throws Exception {
            FileSystem fs = FileSystem.get(URI.create(HDFS_ROOT_URL),conf);
            Path path = new Path(filePath);
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            StringBuilder builder = new StringBuilder();
            while((line = br.readLine())!= null){
                builder.append(line);
            }
            in.close();
            br.close();
            fs.close();
            return builder.toString();
        }
    }

    public static ArrayList<String> parse(String st){
        String s = st.replace("\tArrayWritable ","");
        s=s.replace("valueClass=class org.apache.hadoop.io.Text, values=","");
        s=s.replace("service-","");
        ArrayList<String> list = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        for (int i =0; i<s.length();i++){
            char c = s.charAt(i);
            if(isDigit(c) || c=='.'){
                builder.append(c);
            }
            else {
                if (!builder.toString().equals(""))
                    list.add(builder.toString());
                builder = new StringBuilder();
            }
        }
        return list;
    }
}
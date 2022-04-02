package com.healthmonitor.healthmonitorbackend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import static com.healthmonitor.healthmonitorbackend.HomeController.HDFSDemo.HDFS_ROOT_URL;

@RestController
@RequestMapping("/api")
@CrossOrigin
public class HomeController {
    @GetMapping("/getstatistics")
    public ArrayList<String> getStatistics(@RequestParam String startDate, @RequestParam String endDate) throws Exception {
        System.out.println("I am hereeeeeee");
        ArrayList<String> test = new ArrayList<>();
        test.add(startDate);
        test.add(endDate);
        HDFSDemo demo = new HDFSDemo();
        String path = "/batch_0.log";
        String content = demo.printHDFSFileContents(path);
        System.out.println(content);
        return test;
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
}
//
package com.healthmonitor.healthmonitorbackend.spark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.healthmonitor.healthmonitorbackend.spark.JsonFileReader;
import com.healthmonitor.healthmonitorbackend.spark.Message;
import com.healthmonitor.healthmonitorbackend.spark.MessageDTO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;

public class Main {

    public static void main(String[] args) throws Exception {
        int counter = 0;
        int hadoopCounter = 1;

        while(true) {

            System.out.println("Sending from file number " + counter);
            String file = "/home/mohamed-yasser/health_data/health_" + counter + ".json";
            String json = JsonFileReader.readFileAsString(file);
            String replacedString = json.replaceAll("}\\{", "},{");

            StringBuilder builder = new StringBuilder("[");
            builder.append(replacedString);
            builder.append("]");
            Type listType = new TypeToken<List<Message>>() {
            }.getType();

            List<Message> messages = new Gson().fromJson(builder.toString(), listType);
            out.println("Batch arrived.. sending to hadoop");
            Configuration conf = new Configuration();
            conf.set("dfs.replication", "1");
            FileSystem hdfs = FileSystem.get(new URI("hdfs://hadoop-master:9000"),conf);
//            LocalDate date = LocalDate.now();
            Path hadoopFile = new Path("hdfs://hadoop-master:9000/inputs/" + "data_" + hadoopCounter + ".csv");
            if ( hdfs.exists( hadoopFile )) {
                out.println("file is found");
                long start = System.nanoTime();
                OutputStream os = hdfs.append(hadoopFile);
                BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                for (int i = 0; i < messages.size(); i++) {
                    br.write(messages.get(i).getTimeStamp() + "," + messages.get(i).getServiceName() + "," + messages.get(i).getCpu() + "," + messages.get(i).getRam().getTotal() + "," + messages.get(i).getRam().getFree() + "," + messages.get(i).getDisk().getTotal() + "," + messages.get(i).getDisk().getFree());
                }
                br.close();
                out.println("Appended succesfully");
            } else {
                long start = System.nanoTime();
                OutputStream os = hdfs.create( hadoopFile);
                BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

                Type typeList = new TypeToken<List<Message>>() {}.getType();
                for (int i = 0; i < messages.size() - 1; i++) {
                    br.write(messages.get(i).getTimeStamp() + "," + messages.get(i).getServiceName() + "," + messages.get(i).getCpu() + "," + messages.get(i).getRam().getTotal() + "," + messages.get(i).getRam().getFree() + "," + messages.get(i).getDisk().getTotal() + "," + messages.get(i).getDisk().getFree() + "\n");
                }
                br.write( messages.get(messages.size() - 1).getTimeStamp() + "," +messages.get(messages.size() - 1).getServiceName() + "," + messages.get(messages.size() - 1).getCpu() + "," + messages.get(messages.size() - 1).getRam().getTotal() + "," + messages.get(messages.size() - 1).getRam().getFree() + "," + messages.get(messages.size() - 1).getDisk().getTotal() + "," + messages.get(messages.size() - 1).getDisk().getFree() + "\n");

                br.close();

                long finish = System.nanoTime();
                long timeElapsed = finish - start;
                double elapsedTimeInSecond = (double) timeElapsed / 1_000_000_000;
//                out.println("Time taken to write data to hadoop is " + elapsedTimeInSecond + " seconds");
                out.println("Created succesfully");
            }
            hdfs.close();
            //System.out.println(messages.get(0).toJsonString());


//            ArrayList<String> messages=new ArrayList<>();
//            HealthMessageGenerator generator=new HealthMessageGenerator();

//            int i=100;
//            while(i>0){
//                messages.add(generator.generateMessage());
//                i--;
//            }

            for (int  i = 0; i < messages.size(); i++) {

                double ram_utilization = messages.get(i).RAM.total-messages.get(i).RAM.free;
                double disk_utilization = messages.get(i).Disk.total-messages.get(i).Disk.free;

                MessageDTO msg = new MessageDTO(messages.get(i).serviceName,messages.get(i).Timestamp,messages.get(i).CPU,ram_utilization,disk_utilization);

                String s=msg.toJsonString().replaceAll("Timestamp","Stamp");
                send_packet(s);
                //System.out.println(msg.toJsonString());

                System.out.println("Sending packet " + i);
                Thread.sleep(3000);
            }
        }
    }

    public static void send_packet(String s) throws IOException {
        int server_port = 3500;
        DatagramSocket datagramSocket = new DatagramSocket();
        InetAddress server_address = InetAddress.getLocalHost();
        DatagramPacket datagramPacket = new DatagramPacket(s.getBytes(StandardCharsets.UTF_8) , s.length() , server_address , server_port );
        datagramSocket.send(datagramPacket);
        datagramSocket.close();
    }
}

package com.company;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import model.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static file.handler.JsonFileReader.readFileAsString;
import static java.lang.System.out;

public class Main {
    private static Integer msgNo = 1024;

    public static void main(String[] args) throws Exception {
	    //establish connection
//        int port = 3500;
//        DatagramSocket socket = new DatagramSocket(port);
        out.println("Waiting....");
        int hadoopCounter = 1;
        int dataCounter = 1;

        while(true){
            long Totalstart = System.nanoTime();
            out.println("Reading file " + dataCounter);
            String dataFile = "/media/hadoopuser/College/College Labs/Big Data Systems/health_data/health_" + dataCounter + ".json";
            String json = readFileAsString(dataFile);
            String replacedString = json.replaceAll("}\\{", "},{");
            StringBuilder builder = new StringBuilder("[");
            builder.append(replacedString);
            builder.append("]");
            Type listType = new TypeToken<List<Message>>() {
            }.getType();
            List<Message> clientMessages = new Gson().fromJson(builder.toString(), listType);
            System.out.println(clientMessages.get(0).toJsonString());
//            ArrayList<String> messages = messageBatch(socket);
//            for (int i = 0; i < clientMessages.size(); i++) {
//                messages.add(clientMessages.get(i).toJsonString());
//                if(messages.size() == msgNo )
//                    break;
//            }
            out.println("Batch arrived.. sending to hadoop");
            Configuration conf = new Configuration();
            conf.set("dfs.replication", "1");
            FileSystem hdfs = FileSystem.get(new URI("hdfs://hadoop-master:9000"),conf);
//            LocalDate date = LocalDate.now();
            Path file = new Path("hdfs://hadoop-master:9000/inputs/" + "data_" + hadoopCounter + ".csv");
            if ( hdfs.exists( file )) {
                out.println("file is found");
                long start = System.nanoTime();
                OutputStream os = hdfs.append(file);
                BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                for (int i = 0; i < clientMessages.size(); i++) {
                    br.write(clientMessages.get(i).getTimeStamp() + "," + clientMessages.get(i).getServiceName() + "," + clientMessages.get(i).getCpu() + "," + clientMessages.get(i).getRam().getTotal() + "," + clientMessages.get(i).getRam().getFree() + "," + clientMessages.get(i).getDisk().getTotal() + "," + clientMessages.get(i).getDisk().getFree());
                }
                br.close();
                long finish = System.nanoTime();
                long timeElapsed = finish - start;
                double elapsedTimeInSecond = (double) timeElapsed / 1_000_000_000;
//                out.println("Time taken to write data to hadoop is " + elapsedTimeInSecond + " seconds");
                out.println("Appended succesfully");
            } else {
                long start = System.nanoTime();
                OutputStream os = hdfs.create( file);
                BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

                Type typeList = new TypeToken<List<Message>>() {}.getType();
                for (int i = 0; i < clientMessages.size() - 1; i++) {
                        br.write(clientMessages.get(i).getTimeStamp() + "," + clientMessages.get(i).getServiceName() + "," + clientMessages.get(i).getCpu() + "," + clientMessages.get(i).getRam().getTotal() + "," + clientMessages.get(i).getRam().getFree() + "," + clientMessages.get(i).getDisk().getTotal() + "," + clientMessages.get(i).getDisk().getFree() + "\n");
                }
                br.write( clientMessages.get(clientMessages.size() - 1).getTimeStamp() + "," +clientMessages.get(clientMessages.size() - 1).getServiceName() + "," + clientMessages.get(clientMessages.size() - 1).getCpu() + "," + clientMessages.get(clientMessages.size() - 1).getRam().getTotal() + "," + clientMessages.get(clientMessages.size() - 1).getRam().getFree() + "," + clientMessages.get(clientMessages.size() - 1).getDisk().getTotal() + "," + clientMessages.get(clientMessages.size() - 1).getDisk().getFree() + "\n");

                br.close();

                long finish = System.nanoTime();
                long timeElapsed = finish - start;
                double elapsedTimeInSecond = (double) timeElapsed / 1_000_000_000;
//                out.println("Time taken to write data to hadoop is " + elapsedTimeInSecond + " seconds");
                out.println("Created succesfully");
            }
            dataCounter++;
            long finish = System.nanoTime();
            long timeElapsed = finish - Totalstart;
            double elapsedTimeInSecond = (double) timeElapsed / 1_000_000_000;
//            out.println("Total time is " + elapsedTimeInSecond + "seconds / batch");
//            out.println("Total throughput is " + 1024 / elapsedTimeInSecond + " records/second");
            hdfs.close();
        }

    }
    public static String receive_packet(DatagramSocket socket) throws IOException {
        byte[] buffer = new byte[256];
        DatagramPacket packet = new DatagramPacket(buffer,buffer.length);

        socket.receive(packet);

        String s = new String(packet.getData(),0,packet.getLength());
        return s;
    }
    public static ArrayList<String> messageBatch(DatagramSocket socket) throws IOException {

        ArrayList<String> messages = new ArrayList<>();
        while(true){
            String msg = receive_packet(socket);
            messages.add(msg);
            out.println(messages.size());
            if(messages.size() == msgNo )
                break;
        }
        return messages;
    }
}

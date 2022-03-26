package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.ArrayList;

import static java.lang.System.out;

public class Main {
    private static int msgNo = 1024;

    public static void main(String[] args) throws IOException, URISyntaxException {
	    //establish connection
        int port = 3500;
        DatagramSocket socket = new DatagramSocket(port);
        out.println("Waiting....");

        while(true){
            ArrayList<String> messages = messageBatch(socket);
            out.println("Batch arrived.. sending to hadoop");
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(new URI("hdfs://hadoop-master:9000"),conf);
            LocalDate date = LocalDate.now();
            Path file = new Path("hdfs://hadoop-master:9000/" + date + "/" + "data.log");
            if ( hdfs.exists( file )) {
                out.println("file is found");
                OutputStream os = hdfs.append(file);
                BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                br.write(messages.toString());
                br.close();
                hdfs.close();
                out.println("Appended succesfully");
            } else {
                OutputStream os = hdfs.create( file);
                BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                br.write(messages.toString());
                br.close();
                hdfs.close();
                out.println("Created succesfully");
            }
        }
        //close connection
        //socket.close();

    }
    public static String receive_packet(DatagramSocket socket) throws IOException {
        byte[] buffer = new byte[256];
        DatagramPacket packet = new DatagramPacket(buffer,buffer.length);

        socket.receive(packet);

        String s = new String(packet.getData(),0,packet.getLength());
          //System.out.println("The Message is " + s );
//        InetAddress clientAddress = packet.getAddress();
//        int clientPort = packet.getPort();
//        System.out.println("Client address : " + clientAddress);
//        System.out.println("Client port : " + clientPort);
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

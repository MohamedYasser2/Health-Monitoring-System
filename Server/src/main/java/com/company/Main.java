package main.java.com.company;

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
//            FileSystem fs = FileSystem.get(conf);
//            FSDataOutputStream stream = fs.create(new     Path("/"+date.toString()+"/data.log"));
//            stream.write(Integer.parseInt(messages.toString()));
//            stream.flush();
//            stream.sync();
//            stream.close();
            Path file = new Path("hdfs://hadoop-master:9000/" + date.toString() + "/" + date.toString() +".log");
            if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
            OutputStream os = hdfs.create( file);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            br.write(messages.toString());
            br.close();
            hdfs.close();
            out.println("Created succesfully");
//            String directoryName = "/"+date.toString()+"/"+ file;
//            Path path = new Path(directoryName);
//            fileSystem.mkdirs(path);
//            if(fileSystem instanceof DistributedFileSystem) {
//                out.println("HDFS is the underlying filesystem");
//            }
//            else {
//                out.println("Other type of file system "+fileSystem.getClass());
//            }
            out.println("file created successfully !");
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

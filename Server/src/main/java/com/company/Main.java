package com.company;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;

public class Main {
    private static int msgNo = 5;

    public static void main(String[] args) throws IOException {
	    //establish connection
        int port = 3500;
        DatagramSocket socket = new DatagramSocket(port);
        System.out.println("Waiting....");

        while(true){
            ArrayList<String> messages = messageBatch(socket);
            System.out.println("Batch arrived");
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
            System.out.println(messages.size());
            if(messages.size() == msgNo )
                break;
        }
        return messages;
    }
}

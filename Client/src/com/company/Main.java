package com.company;

import health_message.HealthMessageGenerator;
import health_message.IHealthMessageGenerator;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        // write your code here
        IHealthMessageGenerator healthMessageGenerator = new HealthMessageGenerator();
        int i = 1;
        while(true) {
            send_packet(healthMessageGenerator.generateMessage());
            System.out.println("Sending packet " + i);
            i++;
            Thread.sleep(10);
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

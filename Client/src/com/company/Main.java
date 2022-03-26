package com.company;

import health_message.HealthMessageGenerator;
import health_message.IHealthMessageGenerator;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class Main {

    public static void main(String[] args) throws IOException {
        // write your code here
        IHealthMessageGenerator healthMessageGenerator = new HealthMessageGenerator();
        for(int i=0;i<2048;i++)
            send_packet(healthMessageGenerator.generateMessage());
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

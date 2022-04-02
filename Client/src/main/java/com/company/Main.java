package com.company;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import model.Message;

import java.io.*;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static file.handler.JsonFileReader.readFileAsString;

public class Main {

    public static void main(String[] args) throws Exception {
        int counter = 0;
        while(true) {
            System.out.println("Sending from file number " + counter);
            String file = "/media/hadoopuser/College/College Labs/Big Data Systems/health_data/health_" + counter + ".json";
            String json = readFileAsString(file);
            String replacedString = json.replaceAll("}\\{", "},{");
            StringBuilder builder = new StringBuilder("[");
            builder.append(replacedString);
            builder.append("]");
            Type listType = new TypeToken<List<Message>>() {
            }.getType();
            List<Message> messages = new Gson().fromJson(builder.toString(), listType);
            System.out.println(messages.get(0).toJsonString());

            for (int i = 0; i < messages.size(); i++) {
                send_packet(messages.get(i).toJsonString());
                System.out.println("Sending packet " + i);
                i++;
                Thread.sleep(10);
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

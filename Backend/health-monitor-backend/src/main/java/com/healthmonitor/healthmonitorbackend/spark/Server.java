package com.healthmonitor.healthmonitorbackend.spark;// Java program to illustrate Server side
// Implementation using DatagramSocket
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;

public class Server
{
    public static void main(String[] args) throws IOException {
        DatagramSocket ds = new DatagramSocket(3500);
        byte[] receive = new byte[65535];
        StringBuilder[] messages=new StringBuilder[10];
        DatagramPacket DpReceive = null;
        int fileNo=1;
        while (true)
        {
            for(int i=0;i< messages.length;i++){
                DpReceive = new DatagramPacket(receive, receive.length);
                ds.receive(DpReceive);

                //System.out.println("Client:-" + data(receive));
                messages[i]=data(receive);
                receive = new byte[65535];
            }
            FileWriter myWriter = new FileWriter("json/test"+fileNo+".json");
            fileNo++;
            for(int i=0;i< messages.length;i++){
                if(i==0)
                    myWriter.write("["+messages[i]+",");
                else if(i==messages.length-1)
                    myWriter.write(messages[i]+"]");
                else
                    myWriter.write(messages[i]+",");
            }
            myWriter.close();
            messages=new StringBuilder[10];
        }
    }
    public static StringBuilder data(byte[] a)
    {
        if (a == null)
            return null;
        StringBuilder ret = new StringBuilder();
        int i = 0;
        while (a[i] != 0) {
            ret.append((char) a[i]);
            i++;
        }
        return ret;
    }
}
package com.scu.coen317;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.*;

import java.util.*;
import java.io.*;

public class Producer {
    String ip;
    int port;
    ServerSocket receiveSocket;

    List<Broker> brokersCache;

    // topic, <partition, 負責的broker>
    Map<String, List<Pair<Integer,Broker>> > topic_partition_leaders;

    public Producer (String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        receiveSocket = new ServerSocket(port);
        brokersCache = new ArrayList<>();
        topic_partition_leaders = new HashMap<>();
    }

    private int hashCode(String msg) {
        int hash = 5381;
        int i = 0;
        while (i < msg.length()) {
            hash = ((hash << 5) + hash) + msg.charAt(i++); /* hash * 33 + c */
        }
        return hash;
    }

    public void sendMessage(String topic, String msg) throws IOException {
        //String sentence;
        List<Pair<Integer,Broker>> ls= topic_partition_leaders.get(topic);
        if ( topic_partition_leaders.get(topic) == null ) {
            // 先問default broker list
        }
        //int totalPartition = topic_partition_leaders.get(topic).size();
        int partition = hashCode(msg) % 4;
        
        /*String modifiedSentence;
        Topic t1 = new Topic("Test1", 1,1);
        Topic t2 = new Topic("Test2", 1,1);
        List<Object> mylist = new ArrayList<>();
        mylist.add(t1);
        mylist.add(t2);
        mylist.add("hi");
        //BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket = new Socket("localhost",this.port);
        ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //sentence = inFromUser.readLine();
        //outToServer.writeBytes(sentence + '\n');

        outToServer.writeObject(mylist);
        modifiedSentence = inFromServer.readLine();
        System.out.println("FROM SERVER: " + modifiedSentence);
        clientSocket.close();*/
    }
    /*public void sendMessage(Topic topic, String msg) throws IOException {
        String metaData;
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket = new Socket("localhost", 6789);
        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        sentence = inFromUser.readLine();
        outToServer.writeBytes(sentence + '\n');
        modifiedSentence = inFromServer.readLine();
        System.out.println("FROM SERVER: " + modifiedSentence);
        clientSocket.close();
    }
    /*public void sendMessage(Topic topic, String msg) throws IOException {
        String metaData;
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket = new Socket(ip, port);

        ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
        BufferedReader inFromServer =
                new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        //sentence = inFromUser.readLine();
        outToServer.writeObject(topic);
        outToServer.writeBytes(msg + '\n');
        metaData = inFromServer.readLine();
        System.out.println("FROM SERVER: " + metaData);

        clientSocket.close();
    }*/
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 9000);
        p.sendMessage("topic", "haha");
    }
}
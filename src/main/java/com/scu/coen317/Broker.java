package com.scu.coen317;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Broker {
    String ip;
    int port;
    ServerSocket receiveSocket;

    // 某topic, partition 的其他組員是誰
    Map<String, List<Broker>> topicsMember;

    // 作为coordinator要用到的讯息
    Map<String, Broker> topics_coordinator;

    // each topic's consumer group leader
    Map<String, Consumer> consumerLeader;

    // 记录consumer， offset
    Map<Consumer, Integer> consumerOffset;

    public Broker(String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        receiveSocket = new ServerSocket(port);
        topicsMember = new HashMap();
        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();
    }
    public void receive_msg() throws IOException {
        String clientSentence;
        String capitalizedSentence;
        ServerSocket welcomeSocket = new ServerSocket(6789);
        while(true) {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

            DataOutputStream outToClient = new DataOutputStream (connectionSocket.getOutputStream());
            clientSentence = inFromClient.readLine();
            capitalizedSentence = clientSentence.toUpperCase() + '\n';
            outToClient.writeBytes (capitalizedSentence);
        }
    }

    public static void main(String argv[]) throws Exception {
        Broker b = new Broker("localhost", 6789);
        b.receive_msg();
    }
}


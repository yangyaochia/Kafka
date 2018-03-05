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

import static java.lang.Thread.sleep;

public class Producer {
    String host;
    int port;
    //TcpClient sock;
    //TcpClientEventHandler handler;
    // default brokers and broker cache
    Broker defaultBroker;
    List<Broker> brokers;

    // topic, <partition, 負責的broker>
    Map<String, List<Pair<Integer,Broker>> > topicPartitionLeaders;

    public Producer (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.host = host;
        this.port = port;

        //sock.setReadInterval(5000);


        defaultBroker = new Broker(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(defaultBroker);

        topicPartitionLeaders = new HashMap<>();

    }

    private int hashCode(String msg) {
        int hash = 5381;
        int i = 0;
        while (i < msg.length()) {
            hash = ((hash << 5) + hash) + msg.charAt(i++); /* hash * 33 + c */
        }
        return hash;
    }

    public void sendMessage(String topic, String msg) throws IOException, InterruptedException {
        //sock.connect();
        List<Object> request = new ArrayList<>();
        request.add(topic);
        Topic t = new Topic(topic);
        request.add(t);
        request.add(msg);

        TcpClient sock = new TcpClient(host, port);
        final TcpClient that_sock = sock;
        sock.addEventHandler(new TcpClientEventHandler(){
            public void onMessage(List<Object> msg){
                //handler.onMessage(cid, msg);
                System.out.println((String)msg.get(0));
            }
            public void onOpen(){
                System.out.println("* socket connected");

                // send
                int count = 1;
                while(true){
                    that_sock.send(request);
                    if(count < 1){
                        that_sock.close();
                        break;
                    }
                    count--;
                    try{
                        Thread.sleep(1000);
                    }
                    catch(Exception ex){
                        ex.printStackTrace();
                    }
                }
            }
            public void onClose(){
                //handler.onClose(cid);
            }
        });
        List<Pair<Integer,Broker>> ls= topicPartitionLeaders.get(topic);
        if ( topicPartitionLeaders.get(topic) == null ) {
            // 先問default broker list

        }
        //int totalPartition = topic_partition_leaders.get(topic).size();
        int partition = hashCode(msg) % 4;

        sock.run();
    }
    public void updateTopicPartitionLeader(String topic, List<Pair<Integer,Broker>> partitionLeaders) {
        topicPartitionLeaders.put(topic, partitionLeaders);
    }

    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 9000, "localhost", 9001);
        p.sendMessage("topic1", "1");
        //sleep(1000);
        p.sendMessage("topic2", "2");
        //sleep(1000);
        p.sendMessage("topic3", "3");
        //sleep(1000);
    }
}
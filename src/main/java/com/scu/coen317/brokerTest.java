package com.scu.coen317;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.*;

import java.util.*;
import java.io.*;

import static java.lang.Thread.sleep;

public class brokerTest{
    String host;
    int port;
    //TcpClient sock;
    //TcpClientEventHandler handler;
    // default brokers and broker cache
    Broker defaultBroker;
    List<Broker> brokers;

    // topic, <partition, 負責的broker>
    Map<String, List<Pair<Integer,Broker>> > topicPartitionLeaders;
    private int partition;

    public brokerTest (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
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

    public void sendMessage(String topic, String msg) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        //sock.connect();

        List<Object> argument = new ArrayList<>();
        argument.add(topic);
        argument.add(msg);
        Message message = new Message(MessageType.GET_TOPIC,argument);

        TcpClient sock = new TcpClient(defaultBroker.host, defaultBroker.port);
//        sock.setReadInterval(1000);
        sock.setHandler(this.getClass(), this, message);
        List<Pair<Integer,Broker>> ls= topicPartitionLeaders.get(topic);
        if ( topicPartitionLeaders.get(topic) == null ) {
            // 先問default broker list

        }
        //int totalPartition = topic_partition_leaders.get(topic).size();
        int partition = hashCode(msg) % 4;
        sock.run();
    }



    public void topicAssignmentToProduer(String message) {
        System.out.println(message);


        return;
    }

    public void updateTopicPartitionLeader(String topic, List<Pair<Integer,Broker>> partitionLeaders) {
        topicPartitionLeaders.put(topic, partitionLeaders);
    }

    public void update(String s) {
        System.out.println("received response from broker");
        return;
    }

    public static void main(String argv[]) throws Exception {
        brokerTest p = new brokerTest("localhost", 9003, "localhost", 9000);
        p.sendMessage("topic1", "1");
        //sleep(1000);
        p.sendMessage("topic2", "2");
        //sleep(1000);
        p.sendMessage("topic3", "3");
        //sleep(1000);

    }
}

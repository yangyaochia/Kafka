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

public class Producer {
    String host;
    int port;
    //TcpClient sock;
    //TcpClientEventHandler handler;
    // default brokers and broker cache
    //Broker defaultBroker;
    HostRecord defaultBroker;
    List<HostRecord> brokers;
    Map<String, Map<Integer,HostRecord>> topicsMember;
    Set<Topic> publishTopicSet;


    public Producer (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.host = host;
        this.port = port;

        defaultBroker = new HostRecord(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(defaultBroker);

        topicsMember = new HashMap<>();
        publishTopicSet = new HashSet<>();
    }

    private int hashCode(String msg) {
        int hash = 5381;
        int i = 0;
        while (i < msg.length()) {
            hash = ((hash << 5) + hash) + msg.charAt(i++); /* hash * 33 + c */
        }
        return hash;
    }
    public void createTopic(String topic, int partition, int replication) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        List<Object> argument = new ArrayList<>();
        Topic t = new Topic(topic, partition, replication);
        argument.add(t);
        publishTopicSet.add(t);
        Message message = new Message(MessageType.CREATE_TOPIC, argument);

        TcpClient sock = new TcpClient(defaultBroker.getHost(), defaultBroker.getPort());
        sock.setHandler(this.getClass(), this, message);
        sock.run();

    }
    public void updateTopicPartitionLeader(Topic topic, Map<Integer,HostRecord> partitionLeaders) {
        System.out.println("Hello!!!");
        topicsMember.put(topic.getName(), partitionLeaders);
        publishTopicSet.add(topic);

        return;
    }

    public void publishMessage(String topic, String message) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        //sock.connect();

        if ( !topicsMember.containsKey(topic) ) {
            // Reuse the createTopic function to get corresponding topic partition leaders
            createTopic(topic,1,1);
        }
        int partition = hashCode(message) % topicsMember.get(topic).size();
        HostRecord partitionLeader = topicsMember.get(topic).get(partition);
        List<Object> argument = new ArrayList<>();
        argument.add(topic);
        argument.add(partition);
        argument.add(message);
        Message request = new Message(MessageType.PUBLISH_MESSAGE, argument);

        TcpClient sock = new TcpClient(partitionLeader.getHost(), partitionLeader.getPort());
//        sock.setReadInterval(1000);
        sock.setHandler(this.getClass(), this, request);
        sock.run();
    }

    public void publishMessageAck(String message) {
        System.out.println(message);


        return;
    }

    public void update(String s) {
        System.out.println("received response from broker");
        return;
    }

    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8000, "localhost", 9000);
        p.createTopic("topic1", 2,2);
//        p.publishMessage("topic1", "1");
//        //sleep(1000);
//        p.publishMessage("topic2", "2");
//        //sleep(1000);
//        p.publishMessage("topic3", "3");
        //sleep(1000);
    }
}
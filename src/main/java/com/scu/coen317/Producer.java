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

    // Set of default brokers
    // If the producer does not know whom to contact
    //
    Set<HostRecord> defaultBrokers;

    Map<String, Map<Integer,HostRecord>> topicsMember;
    Set<String> publishTopicSet;

    public Producer (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.host = host;
        this.port = port;

        HostRecord h = new HostRecord(defaultBrokerIp, defaultBrokerPort);
        defaultBrokers = new HashSet<>();
        defaultBrokers.add(h);

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
    public boolean createTopic(String topic, int partition, int replication) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {

        if ( !topicsMember.containsKey(topic) ) {
            List<Object> argument = new ArrayList<>();
            Topic t = new Topic(topic, partition, replication);
            argument.add(t);
            publishTopicSet.add(topic);
            Message request = new Message(MessageType.CREATE_TOPIC, argument);
            HostRecord defaultBroker = defaultBrokers.iterator().next();
            TcpClient sock = new TcpClient(defaultBroker.getHost(), defaultBroker.getPort());
            sock.setHandler( this, request);
            //sock.setReadInterval(2000);
//        System.out.println(sock.getReadInterval());
            sock.run();
            return true;
        } else {
            // This topic has already been created.
            return false;
        }
    }

    public void updateTopicPartitionLeader(String topic, HashMap<Integer,HostRecord> partitionLeaders) {
        System.out.println("haha finally" + topic);
        topicsMember.put(topic, partitionLeaders);
        publishTopicSet.add(topic);

        return;
    }



    public void publishMessage(String topic, String message) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Map<Integer,HostRecord> partitionLeaders = new HashMap<>();
        partitionLeaders.put(0, new HostRecord("localhost", 9000));
        partitionLeaders.put(1, new HostRecord("localhost", 9001));
        topicsMember.put(topic, partitionLeaders);
        if ( !topicsMember.containsKey(topic) ) {
            // Reuse the createTopic function to get corresponding topic partition leaders
            createTopic(topic,1,1);
        }

        int partition = hashCode(message) % topicsMember.get(topic).size();
        System.out.println(topic + " " + message + " " + partition);
        HostRecord partitionLeader = topicsMember.get(topic).get(partition);
        System.out.println(partitionLeader.getPort());
        List<Object> argument = new ArrayList<>();
        argument.add(topic);
        argument.add(partition);
        argument.add(message);
        Message request = new Message(MessageType.PUBLISH_MESSAGE, argument);

        TcpClient sock = new TcpClient(partitionLeader.getHost(), partitionLeader.getPort());
        //while ( sock.getCloser() )
        //sock.setReadInterval(1000);
        sock.setHandler( this, request);
        sock.run();
        return;
    }

    public void publishMessageAck(String message, String ackMessage) {
        System.out.println(message + ackMessage);
    }

    public void update(String s) {
        System.out.println("received response from broker");
        return;
    }

    public void addDefaultBroker(String host, Integer port) {
        HostRecord h = new HostRecord(host, port);
        defaultBrokers.add(h);
    }

    public void removeDefaultBroker(String host, Integer port) {
        HostRecord h = new HostRecord(host, port);
        defaultBrokers.remove(h);
    }

}
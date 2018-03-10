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

    boolean ack = false;

    public Producer (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.host = host;
        this.port = port;

        HostRecord h = new HostRecord(defaultBrokerIp, defaultBrokerPort);
        defaultBrokers = new HashSet<>();
        defaultBrokers.add(h);

        topicsMember = new HashMap<>();
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
        // This producer has not store the topic information before
        // Or partition = -1 : the topic partition leader is broken
        // Re-request the topic partition leaders information
        if ( !topicsMember.containsKey(topic) ) {
            List<Object> argument = new ArrayList<>();
            Topic t = new Topic(topic, partition, replication);
            argument.add(t);
//            publishTopicSet.add(topic);
            Message request = new Message(MessageType.CREATE_TOPIC, argument);
            HostRecord defaultBroker = defaultBrokers.iterator().next();
            System.out.println(defaultBroker.getPort());
            TcpClient sock = null;

            // Handle the case : default broker down
            while (!defaultBrokers.isEmpty()) {
                System.out.println(defaultBrokers.size());
                defaultBroker = defaultBrokers.iterator().next();
                try {
                    sock = new TcpClient(defaultBroker.getHost(), defaultBroker.getPort());
                    break;
                } catch (IOException e) {
                    System.out.println("The first default broker is down");
                    defaultBrokers.remove(defaultBrokers.iterator().next());

                }
            }
            if ( defaultBrokers.isEmpty() )
                return false;

            sock.setHandler( this, request);
            sock.run();
            synchronized (this) {
                while (!ack ) {
                    wait();
                }
                ack = false;
                return true;
            }
        } else {
            // This topic has already been created.
            return false;
        }
    }

    public void updateTopicPartitionLeader(String topic, HashMap<Integer,HostRecord> partitionLeaders) {
        System.out.println("haha finally" + topic);
        topicsMember.put(topic, partitionLeaders);

        for (  Map.Entry<Integer, HostRecord> pair : partitionLeaders.entrySet() ) {
            defaultBrokers.add(pair.getValue());
        }

        synchronized (this) {
            ack = true;
            notify();
        }
        return;
    }

    public boolean publishMessage(String topic, String message) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        // Hardcode
        Map<Integer,HostRecord> partitionLeaders = new HashMap<>();
        partitionLeaders.put(0, new HostRecord("localhost", 9000));
        partitionLeaders.put(1, new HostRecord("localhost", 9001));
        topicsMember.put(topic, partitionLeaders);
        // Hardcode
        if ( !topicsMember.containsKey(topic) ) {
            // Reuse the createTopic function to get corresponding topic partition leaders
            createTopic(topic,1,1);
        }
        TcpClient sock = null;
        int partition = hashCode(message) % topicsMember.get(topic).size();
        System.out.println(topic + " " + message + " " + partition);
        HostRecord partitionLeader = null;

        List<Object> argument = new ArrayList<>();
        argument.add(topic);
        argument.add((Integer)partition);
        argument.add(message);
        Message request = new Message(MessageType.PUBLISH_MESSAGE, argument);

        int leaderAliveChance = 1;
        while (leaderAliveChance >= 0) {
            partitionLeader = topicsMember.get(topic).get(partition);
            System.out.println(partitionLeader.getPort());
            try {
                sock = new TcpClient(partitionLeader.getHost(), partitionLeader.getPort());
                sock.setHandler( this, request);
                break;
            } catch (IOException e) {
//                e.printStackTrace();
                topicsMember.remove(topic);
                leaderAliveChance--;
                // To indicate the topic partition leader broken case
                createTopic(topic,1,1);
            }
        }
        if ( leaderAliveChance < 0 )
            return false;
//        sock.setReadInterval(10000);

        sock.run();
        return true;
    }

    public void publishMessageAck(String message, String ackMessage) {
        System.out.println("This is Ack message " + message + " " + ackMessage);
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
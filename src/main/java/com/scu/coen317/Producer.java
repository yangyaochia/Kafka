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
    HostRecord thisProducer;
    Set<HostRecord> defaultBrokers;

    Map<String, Map<Integer,HostRecord>> topicsMember;

    boolean updateTopicPartitionLeaderACK = false;
    boolean publishMessageACK = false;

    final int MONITOR_CLUSTER_INTERVAL = 4000;

    public Producer (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.host = host;
        this.port = port;
        this.thisProducer = new HostRecord(host, port);
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
        if ( !topicsMember.containsKey(topic) ) {
            List<Object> argument = new ArrayList<>();
            Topic t = new Topic(topic, partition, replication);
            argument.add(t);
            argument.add(thisProducer);
            Message request = new Message(MessageType.CREATE_TOPIC, argument);
            HostRecord defaultBroker = defaultBrokers.iterator().next();
            System.out.println("Producer > Send createTopic request to default broker: " + defaultBroker.getPort());
            TcpClient sock = null;

            // Handle the case : default broker down
            while (!defaultBrokers.isEmpty()) {
                defaultBroker = defaultBrokers.iterator().next();
                try {
                    sock = new TcpClient(defaultBroker.getHost(), defaultBroker.getPort());
                    break;
                } catch (IOException e) {
                    System.out.println("Producer > The first default broker " + defaultBroker.getPort() + " is not open.");
                    defaultBrokers.remove(defaultBrokers.iterator().next());
                }
            }
            
            if (defaultBrokers.isEmpty()) {
                return false;
            }
            
            TcpServer listenSock = new TcpServer(this.port);
            listenSock.setHandler(this);
            listenSock.listen();
            sock.setHandler( this, request);
            sock.run();

            synchronized (this) {
                while (!updateTopicPartitionLeaderACK ) {
                    wait();
                }
                updateTopicPartitionLeaderACK = false;
                listenSock.close();
            }
            return true;
        } else {
            // This topic has already been created.
            return false;
        }
    }

    public void updateTopicPartitionLeader(String topic, HashMap<Integer,HostRecord> partitionLeaders) {
        System.out.println("Producer > Receiving brokers information......");
        topicsMember.remove(topic);
        topicsMember.put(topic, partitionLeaders);
        
        for ( Map.Entry<Integer,HostRecord> pair : topicsMember.get(topic).entrySet() ) {
            System.out.println("Producer > This topic is : '" + topic + "' partition : " + pair.getKey() + "  Broker : " + pair.getValue());
            defaultBrokers.add(pair.getValue());
        }
        
        synchronized (this) {
            updateTopicPartitionLeaderACK = true;
            notify();
        }
        return;
    }

    public boolean publishMessage(String topic, String message) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        while ( !topicsMember.containsKey(topic) ) {
            // Reuse the createTopic function to get corresponding topic partition leaders
            createTopic(topic,1,1);
        }
        
        TcpClient sock = null;
        Integer partition = (hashCode(message)) % topicsMember.get(topic).size();
        
        if ( partition < 0 ) {
            partition += topicsMember.get(topic).size();
        }
        
        HostRecord partitionLeader = null;
        List<Object> argument = new ArrayList<>();
        argument.add(topic);
        argument.add(partition);
        argument.add(message);
        argument.add(thisProducer);
        Message request = new Message(MessageType.PUBLISH_MESSAGE, argument);

        while ( true ) {
            partitionLeader = topicsMember.get(topic).get(partition);
            try {
                sock = new TcpClient(partitionLeader.getHost(), partitionLeader.getPort());
                break;
            } catch (IOException e) {
                topicsMember.remove(topic);
                // To indicate the topic partition leader broken case
                Thread.sleep((long) (MONITOR_CLUSTER_INTERVAL*1.5));
                createTopic(topic,1,1);
            }
        }

        System.out.println("Producer > Sending message " + message + " to " + partitionLeader.getPort());
        TcpServer listenSock = new TcpServer(this.port);
        listenSock.setHandler(this);
        listenSock.listen();
        sock.setHandler( this, request);
        sock.run();
        waitInvokeFunction(listenSock);
        return true;
    }

    public void waitInvokeFunction(TcpServer listenSock) throws InterruptedException {
        synchronized (this) {
            while (!publishMessageACK) {
                wait();
            }
            listenSock.close();
        }
    }

    public void publishMessageAck(String message, String ackMessage) {
        synchronized (this) {
            publishMessageACK = true;
            notify();
        }
    }

    public void addDefaultBroker(String host, Integer port) {
        HostRecord h = new HostRecord(host, port);
        defaultBrokers.add(h);
    }

    public void removeDefaultBroker(String host, Integer port) {
        HostRecord h = new HostRecord(host, port);
        defaultBrokers.remove(h);
    }

    public void printDefaultBrokerList() {
        System.out.println("Producer > Default broker list");
        for ( HostRecord h : defaultBrokers ) {
            System.out.println("\t" + h);
        }
    }
}

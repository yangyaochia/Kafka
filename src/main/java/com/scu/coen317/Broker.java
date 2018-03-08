package com.scu.coen317;

import javafx.util.Pair;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;

public class Broker {
    String host;
    int port;
    TcpServer listenSocket;

    Zookeeper defaultZookeeper;

//    TcpServerEventHandler serverHandler;
    // 某topic, partition 的其他組員是誰
    Map<String, Map<Integer,List<String>>> topicMessage;
    Map<String, Map<Integer,HostRecord>> topicsPartitionLeader;

    // Map<topic,Map<partition,List<replicationHolders>>
    Map<String,Map<String,List<HostRecord>>> topicPartitionReplicationBrokers;
    Map<String, HostRecord> topics_coordinator;
    // 作为coordinator要用到的讯息
    Map<String, List<HostRecord>> topic_consumer;
    Map<Consumer, Map<String, List<Pair<Integer, HostRecord>>>> balance;
    // each group's leader
    Map<String, Consumer> consumerLeader;

    // 记录consumer，each topic offset
    Map<Consumer, Map<String,Integer>> consumerOffset;
    

    public Broker(String host, int port, String zookeeperHost, int zookeeperPort) throws IOException {
        this.host = host;
        this.port = port;
        this.defaultZookeeper = new Zookeeper(zookeeperHost, zookeeperPort);

        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);

        topicsPartitionLeader = new HashMap();
        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();
        topicMessage = new HashMap<>();
    }

    public Message getTopic(Topic topic) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {

        String topicName = topic.getName();
        List<Object> argument = new ArrayList<>();
        Message response;
        Map<Integer,HostRecord> leaders = new HashMap<>();
        leaders.put(0, new HostRecord("localhost", 9000));
        leaders.put(1, new HostRecord("localhost", 9000));
        topicsPartitionLeader.put(topicName, leaders);
        // This broker does now know the topic, then ask the zookeeper
        if ( !topicsPartitionLeader.containsKey(topicName) ) {
            argument.add(topic);
            Message request = new Message(MessageType.GET_TOPIC, argument);

            TcpClient sock = new TcpClient(defaultZookeeper.host, defaultZookeeper.port);
            sock.setHandler( this, request);
            sock.run();

        }
        // This broker already stored the topic info
        synchronized (this) {
            while (!topicsPartitionLeader.containsKey(topicName) ) {
                wait();
            }
            argument.add(topicName);
            argument.add(topicsPartitionLeader.get(topicName) );
            response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_PRODUCER, argument);
            return response;
        }

    }
    public void topicAssignmentToProduer(Topic topic, HashMap<Integer,HostRecord> partitionLeaders) {
        synchronized (this) {
            topicsPartitionLeader.put(topic.getName(), partitionLeaders);
            notify();
        }
        return;
    }

    public void setTopicLeader(String topic, Integer partition ) {
        // Structure for topicMessage
        // Map<String, Map<Integer,List<String>>>  Map<topic, Map<partition, List<message>>
        Map<Integer, List<String>> partitionMap = new HashMap<>();
        partitionMap.put(partition, new ArrayList());
        topicMessage.put(topic, partitionMap);
    }

    public Message publishMessage(String topic, Integer partition, String message) {
        System.out.println("Hello??" + "topic map's size is " + topicMessage.size());
        topicMessage.get(topic).get(partition).add(message);



        List<Object> arguments = new ArrayList<>();
        arguments.add(topic);
        arguments.add("Published Successful");
        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK, arguments);
        return response;
    }


    public Message getCoordinator(String groupId) {
//        while (!topics_coordinator.containsKey(groupId)) {
//
//        }
//        HostRecord coordinator = topics_coordinator.get(groupId);
        List<Object> arguments = new ArrayList();
//        arguments.add(coordinator);
        arguments.add(new HostRecord("localhost", this.port));
        Message response = new Message(MessageType.UPDATE_COORDINATOR, arguments);
        return response;
    }



    public Message storeInfoAndGetTopic(String topic, String groupId) throws IOException {
        /*balance = null;
        while (balance == null) {
            Consumer leader = consumerLeader.get(consumerLeader.get(groupId));
            Message request = new Message(MessageType.REBALANCE);
            TcpClient socket = new TcpClient(leader.host, leader.port);
            socket.setHandler(this.getClass(), this, request);
        }
        */

        List<Object> arguments = new ArrayList<>();
        Map<String, HostRecord> map = new HashMap<>();
        map.put("topic1", new HostRecord("localhost", 9000));
        arguments.add(map);
        Message response = new Message(MessageType.REBALANCEPLAN, arguments);
        return response;
    }


    public void listen() throws IOException, ClassNotFoundException {

        listenSocket.listen();
    }


}


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
    Map<String, Map<Integer,List<String>> > topicMessage;
    Map<String, Map<Integer,Broker>> topicsMember;
    Map<String, Broker> topics_coordinator;
    // 作为coordinator要用到的讯息
    Map<String, List<Consumer>> topic_consumer;
    Map<Consumer, Map<String, List<Pair<Integer, Broker>>>> balance;
    // each group's leader
    Map<String, Consumer> consumerLeader;

    // 记录consumer，each topic offset
    Map<Consumer, Map<String,Integer>> consumerOffset;
    

    public Broker(String host, int port, String zookeeperHost, int zookeeperPort) throws IOException {
        this.host = host;
        this.port = port;
        this.defaultZookeeper = new Zookeeper(zookeeperHost, zookeeperPort);

        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this.getClass(), this);

        topicsMember = new HashMap();
        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();
        topicMessage = new HashMap<>();
    }

    public Message getTopic(Topic topic) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {

        String topicName = topic.getName();
        List<Object> argument = new ArrayList<>();
        Message response;

        // This broker does now know the topic, then ask the zookeeper
        if ( !topicsMember.containsKey(topicName) ) {
            argument.add(topic);
            Message request = new Message(MessageType.GET_TOPIC, argument);

            TcpClient sock = new TcpClient(defaultZookeeper.host, defaultZookeeper.port);
            sock.setHandler(this.getClass(), this, request);
            sock.run();

        }
        // This broker already stored the topic info
        synchronized (this) {
            while (!topicsMember.containsKey(topicName) ) {
                wait();
            }
            argument.add(topic);
            argument.add(topicsMember.get(topicName) );
            response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_PRODUCER, argument);
            return response;
        }

    }
    public void topicAssignmentToProduer(Topic topic, Map<Integer,Broker> partitionLeaders) {
        synchronized (this) {
            topicsMember.put(topic.getName(), partitionLeaders);
            notify();
        }
        return;
    }

    public Message publishMessage(String topic, String message) {
        System.out.println("Hello??" + "topic map's size is " + topicMessage.size());

//        System.out.println("This broker's port number :" + this.port);

        List<String> list = topicMessage.getOrDefault(topic, new ArrayList<>());
        list.add(message);
        topicMessage.put(topic, list);

        return publishMessageAck();
    }


    public Message getCoordinator(String groupId) {
        while (!topics_coordinator.containsKey(groupId)) {

        }
        Broker broker = topics_coordinator.get(groupId);
        List<Object> arguments = new ArrayList();
        arguments.add(this.host);
        arguments.add(this.port);
        Message response = new Message(MessageType.UPDATE_COORDINATOR, arguments);
        return response;
    }

    public Message publishMessageAck() {
        List<Object> arguments = new ArrayList<>();
        arguments.add("Successful");
        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK, arguments);
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
        Map<String, Pair<Integer, Broker>> map = new HashMap<>();
        map.put("topic1", new Pair(1, new Broker("localhost", 9000)));
        arguments.add(map);
        Message response = new Message(MessageType.REBALANCEPLAN, arguments);
        return response;
    }


    public void listen() throws IOException, ClassNotFoundException {

        listenSocket.listen();
    }

    public static void main(String argv[]) throws Exception {
        Broker b = new Broker("localhost", 9000, "localhost", 2181);
        b.listen();

    }
}


package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.sql.Timestamp;
import java.util.*;



public class Zookeeper {
    String host;
    int port;
    TcpServer listenSocket;
    Map<String, List<String>> topicMessage;

    // min heap round robin timestamp queue
    // assign brokers when new producer apply a new topic
//    PriorityQueue<Pair<Timestamp,Broker>> clusters;

    Map<String, List<Broker>> topicsMember;

    // 作为coordinator要用到的讯息
    Map<String, Broker> topics_coordinator;

    // each topic's consumer group leader
    Map<String, Consumer> consumerLeader;
    Map<Consumer, Integer> consumerOffset;

    // 记录consumer， offset
    Map<String, Pair<Integer,Broker>> topic_map;

    // 接收來自producer的create_topic
    // 回傳這個topic, partition的負責人給傳的那個人
    public Zookeeper(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this.getClass(), this);
//        clusters = new PriorityQueue<>((p1, p2) -> p1.getKey().compareTo(p2.getKey()));




        topicsMember = new HashMap();
        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();
        topicMessage = new HashMap<>();
    }

    public Message topicAssignment(String topic, String message) {
        System.out.println("Hello??" + "topic map's size is " + topicMessage.size());
        System.out.println("This broker's port number :" + this.port);

        List<String> list = topicMessage.getOrDefault(topic, new ArrayList<>());
        list.add(message);
        topicMessage.put(topic, list);
        return topicAssignmentToBroker();
    }

    public Message topicAssignmentToBroker() {
        List<Object> arguments = new ArrayList<>();
        arguments.add("Successful");
        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
        return response;
    }

    public void addBroker() {

    }

    public void updateCluster() {
        // 新建broker

    }

    public void registerTopic() {
        // socket programming server接資料

        // assign broker and partition

        // communicate to the relative brokers

    }

    // 某個broker來問的
    public List<Pair<Integer,Broker>> responseTopicPartitionLeader(String topic) {
        List<Pair<Integer, Broker>> partitions = new ArrayList();
        // pair of partition and Broker
        return partitions;
    }

    public void listen() throws IOException, ClassNotFoundException {

        listenSocket.listen();
    }

    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 9000);
        z.listen();
    }
}

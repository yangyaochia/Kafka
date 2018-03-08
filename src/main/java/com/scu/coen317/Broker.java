package com.scu.coen317;

import javafx.util.Pair;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;

public class Broker {
    HostRecord thisHost;
    TcpServer listenSocket;

    HostRecord defaultZookeeper;

    // 某topic, partition 的其他組員是誰
    // Map<topic, Map<partition, message>>
    Map<String, Map<Integer,List<String>>> topicMessage;
    Map<String, Map<Integer,HostRecord>> topicsPartitionLeader;

    // Map<topic,Map<partition,List<replicationHolders>>
    Map<String,Map<Integer,Set<HostRecord>>> topicPartitionReplicationBrokers;
    Map<String, HostRecord> topics_coordinator;

    // 作为coordinator要用到的讯息

    // 记录每个topic都是哪些consumer订阅，每个consumer订阅了几个partition
    Map<String, Map<HostRecord, Integer>> topic_consumer;
    // 记录每个group中，每一个consumer订阅的每一个topic都有哪些partition
    Map<String, Map<HostRecord, Map<String, List<Pair<Integer, HostRecord>>>>> balanceMap;
    // each group's leader
    Map<String, HostRecord> consumerLeader;

    // balance Map for each group
    // 记录consumer，each topic offset
    Map<Consumer, Map<String,Integer>> consumerOffset;
    

    public Broker(String host, int port, String zookeeperHost, int zookeeperPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.thisHost = new HostRecord(host, port);
        this.defaultZookeeper = new HostRecord(zookeeperHost, zookeeperPort);

        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);

        topicMessage = new HashMap<>();
        topicsPartitionLeader = new HashMap();
        topicPartitionReplicationBrokers = new HashMap<>();

        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();

        if ( port == 9000 ) {
            Set<HostRecord> replicationHolders = new HashSet<>();
            replicationHolders.add(new HostRecord("localhost", 9001));
            replicationHolders.add(new HostRecord("localhost", 9002));
            setTopicPartitionLeader("topic1", 0, thisHost, (HashSet<HostRecord>) replicationHolders);
            setTopicPartitionLeader("topic1", 1, thisHost, (HashSet<HostRecord>) replicationHolders);
        }

    }
    ////////////////// Yao-Chia
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

            TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
            sock.setHandler( this, request);
            sock.send(request);
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

    public void setTopicPartitionLeader(String topic, Integer partition, HostRecord leader, HashSet<HostRecord> replicationHolders) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        // Structure for topicMessage
        // Map<String, Map<Integer,List<String>>>  Map<topic, Map<partition, List<message>>
        System.out.println(thisHost.getHost() + " " + thisHost.getPort());
        if ( topicMessage.get(topic) == null ) {
            // Case 1 : This broker has not known this topic partition ever
            Map<Integer, List<String>> partitionMap = new HashMap<>();
            partitionMap.put(partition, new ArrayList());
            topicMessage.put(topic, partitionMap);
        } else if ( topicMessage.get(topic).get(partition) == null ) {
            // Case 2: This broker has not known this partition ever
            topicMessage.get(topic).put(partition, new ArrayList<>());
        }

        if ( topicPartitionReplicationBrokers.get(topic) == null ) {
            Map<Integer, Set<HostRecord>> partitionHolderMap = new HashMap<>();
            partitionHolderMap.put(partition, replicationHolders);
            topicPartitionReplicationBrokers.put(topic, partitionHolderMap);
        } else if ( topicPartitionReplicationBrokers.get(topic).get(partition) == null ) {
            topicPartitionReplicationBrokers.get(topic).put(partition, replicationHolders);
        }
        System.out.println("This host: " + thisHost.getPort() + " leader : " + leader.getPort());
        System.out.println("Partition: " + partition );
        System.out.println(thisHost.equals(leader));
        if ( thisHost.equals(leader) ) {
            List<Object> argument = new ArrayList<>();
            argument.add(topic);
            argument.add(partition);
            argument.add(leader);
            argument.add(replicationHolders);
            Message request = new Message(MessageType.SET_TOPIC_PARTITION_LEADER, argument);
            informReplicationHolders(request, replicationHolders);
        }
    }
    public void informReplicationHolders(Message request, HashSet<HostRecord> replicationHolders) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        for ( HostRecord h : replicationHolders ) {
            System.out.println("Send to : " + h.getPort());
            TcpClient sock = new TcpClient(h.getHost(), h.getPort());
            sock.setHandler( this, request);
            sock.connect();
            sock.send(request);
        }
    }

    public Message publishMessage(String topic, Integer partition, String message) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {


        System.out.println("topic map's size is " + topicMessage.get(topic).get(partition).size());
        topicMessage.get(topic).get(partition).add(message);
        System.out.println("topic map's size is " + topicMessage.get(topic).get(partition).size());

        // Send publishMessage to the corresponding topic partition replication holders
        Set<HostRecord> replicationHolders = topicPartitionReplicationBrokers.get(topic).get(partition);
        if ( !replicationHolders.contains(thisHost) ) {
            System.out.println("Hello!!!");
            List<Object> argument = new ArrayList<>();
            argument.add(topic);
            argument.add(partition);
            argument.add(message);
            Message request = new Message(MessageType.PUBLISH_MESSAGE, argument);
            informReplicationHolders(request, (HashSet<HostRecord>) replicationHolders);
        }
//        sock.setReadInterval(1000);


        List<Object> arguments = new ArrayList<>();
        arguments.add(message);
        arguments.add("Published Successful");
        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK, arguments, true);
        return response;
    }

    ////////////////// Yao-Chia


    ////////////////// Xin-Zhu
//    public void registerToZookeeper(HostRecord defaultZookeeper) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
//        TcpClient client = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
//        List<Object> arguments = new ArrayList<>();
//        arguments.add(new HostRecord(this.host, this.port));
//        Message request = new Message(MessageType.NEW_BROKER_REGISTER, arguments);
//        client.setHandler(this,request);
//        client.run();
//    }

    public Message getCoordinator(String groupId) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        while (!topics_coordinator.containsKey(groupId)) {
            TcpClient client = new TcpClient(defaultZookeeper.host,defaultZookeeper.port);
            List<Object> arguments = new ArrayList<>();
            arguments.add(groupId);
            Message request = new Message(MessageType.GET_COORDINATOR, arguments);
            client.setHandler(this, request);
            client.run();
        }
        HostRecord coordinator = topics_coordinator.get(groupId);
        List<Object> arguments = new ArrayList();
        arguments.add(coordinator);
        Message response = new Message(MessageType.UPDATE_COORDINATOR, arguments);
        return response;
    }

    public void updateCoordinator(String groupId, HostRecord coordinator) {
        topics_coordinator.put(groupId, coordinator);
    }

    public Message publishMessageAck() {
        List<Object> arguments = new ArrayList<>();
        arguments.add("Successful");
        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK, arguments);
        response.setIsAck(true);
        return response;
    }

    public Message consumerJoinGroupRegistrationAck() {
        List<Object> arguments = new ArrayList<>();
        arguments.add("Successful");
        Message response = new Message(MessageType.CONSUMER_JOIN_GROUP_REGISTRATION_ACK, arguments);
        response.setIsAck(true);
        return response;
    }

//    public Message rebalance(String groupId) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
//        // coordinator invole rebalance of leader
//
//        HostRecord leader = consumerLeader.get(groupId);
//        TcpClient client = new TcpClient(leader.getHost(),leader.getPort());
//        List<Object> arguments = new ArrayList<>();
//        arguments.add(groupId);
//        Message response = new Message(MessageType.REBALANCE, arguments);
//        client.setHandler(this,response);
//        client.run();
////        Message response = new Message(MessageType.REBALANCEPLAN,)
//    }


    public void rebalance(String groupId) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        balanceMap.remove(groupId);
        // coordinator invole rebalance of leader
        while (!balanceMap.containsKey(groupId)) {
            HostRecord leader = consumerLeader.get(groupId);
            TcpClient client = new TcpClient(leader.getHost(), leader.getPort());
            List<Object> arguments = new ArrayList<>();
            arguments.add(groupId);
            Message response = new Message(MessageType.REBALANCE, arguments);
            client.setHandler(this, response);
            client.run();
        }
        // multicast
        // Map<String, Map<HostRecord, Map<Topic, List<Pair<Integer, HostRecord>>>>> balanceMap;
        Map<HostRecord, Map<String, List<Pair<Integer, HostRecord>>>> map = balanceMap.get(groupId);
        for (HostRecord consumer : map.keySet()) {
            TcpClient client = new TcpClient(consumer.getHost(), consumer.getPort());
            List<Object> arguments = new ArrayList<>();
            arguments.add(map.get(consumer));
            Message request = new Message(MessageType.REBALANCEPLAN, arguments);
            client.setHandler(this, request);
            client.run();
        }
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
////////////////// Xin-Zhu
// //////////////// Hsuan-Chih

// //////////////// Hsuan-Chih

    public void listen() throws IOException, ClassNotFoundException {
        listenSocket.listen();
    }
}


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
    TcpServer listenSocket;
    //TcpClient sock;
    //TcpClientEventHandler handler;
    // default brokers and broker cache
    HostRecord defaultZookeeper ;
//    List<Zookeeper> brokers;

    // topic, <partition, 負責的broker>
    Map<String, List<Pair<Integer,Broker>> > topicPartitionLeaders;
    private int partition;

    public brokerTest (String host, int port, String defaultZookeeperIp, int defaultZookeeperPort) throws IOException {
        this.host = host;
        this.port = port;
        //sock.setReadInterval(5000);
        defaultZookeeper = new HostRecord(defaultZookeeperIp, defaultZookeeperPort);
        topicPartitionLeaders = new HashMap<>();
        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);
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

        TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
//        sock.setReadInterval(1000);
        sock.setHandler(this, message);
//        List<Pair<Integer,Broker>> ls= topicPartitionLeaders.get(topic);
//        if ( topicPartitionLeaders.get(topic) == null ) {
//            // 先問default broker list
//
//        }
//        //int totalPartition = topic_partition_leaders.get(topic).size();
//        int partition = hashCode(msg) % 4;
        sock.run();
    }



    public void registerToZookeeper() throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        //sock.connect();

        List<Object> argument = new ArrayList<>();
        HostRecord temp = new HostRecord(this.host, this.port);
        argument.add(temp);
        Message request = new Message(MessageType.NEW_BROKER_REGISTER,argument);

        TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
//        sock.setReadInterval(1000);
        sock.setHandler( this, request);
        sock.run();

    }

//
////    public void topicAssignmentToProduer(String message) {
////        System.out.println(message);
////
////
////        return;
////    }

    public void getTopic() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {

//        String topic = "hahaha";

        List<Object> argument = new ArrayList<>();
//        Message response;
//        Map<Integer,HostRecord> leaders = new HashMap<>();
//        leaders.put(0, new HostRecord("localhost", 9000));
//        leaders.put(1, new HostRecord("localhost", 9000));
//        topicsPartitionLeader.put(topicName, leaders);

        // This broker does now know the topic, then ask the zookeeper
//        if ( !topicsPartitionLeader.containsKey(topicName) ) {
        Topic topic = new Topic("hahahaha");
        topic.partition = 1;
        topic.replication = 3;
        HostRecord temp = new HostRecord(this.host, this.port);
        argument.add(topic);
        argument.add(temp);
        Message request = new Message(MessageType.GET_TOPIC, argument);


        TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
        sock.setHandler( this, request);
        sock.run();

        // This broker already stored the topic info
//        synchronized (this) {
//            while (!topicsPartitionLeader.containsKey(topicName) ) {
//                wait();
//            }
//            argument.add(topicName);
//            argument.add(topicsPartitionLeader.get(topicName) );
//            response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_PRODUCER, argument);
//            return response;
//        }

    }

    public void getTopicConsumer() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {
        // This broker does not know the topic, then ask the zookeeper
//        if (!topicsPartitionLeaderCache.containsKey(topicName)) {
            List<Object> argument = new ArrayList<>();
            argument.add("hahaha");
            Message request = new Message(MessageType.GET_TOPIC_FOR_COORDINATOR, argument);

            TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
            sock.setHandler(this, request);
            sock.run();

//
    }
    public void updateTopicPartitionLeaderCache(String topic, HashMap<Integer,HostRecord> partitions){
        System.out.println("In topic Assignment to Consumer already... ");
//        System.out.println(msg);
        System.out.println("topic : "+ topic);
        for (Integer name: partitions.keySet()){
            String key =name.toString();
            partitions.get(name).toString();
            System.out.println("Partition " +key.toString() + "  at" + partitions.get(name).getHost() + " "+ partitions.get(name).getPort());
        }

    }
    public void receiveNewBrokerRegistrationAck(String message) {
        System.out.println("hahahaha");
        System.out.println(message);

        return;
    }
    public void updateCoordinator(String groupID, HostRecord message){
        System.out.println("getCoordinator!!!!!!");
        System.out.println(groupID);
        System.out.println(message);
    }

    public void topicAssignmentToProducer(Topic topic, HashMap<Integer,HostRecord> partitions) {
        System.out.println("In topic Assignment to producer already... ");
//        System.out.println(msg);
        System.out.println("topic : "+ topic.getName());
        for (Integer name: partitions.keySet()){
            String key =name.toString();
            partitions.get(name).toString();
            System.out.println("Partition " +key.toString() + "  at" + partitions.get(name).getHost() + " "+ partitions.get(name).getPort());
        }
    }
    public void setTopicPartitionLeader(String topic, Integer partition, HostRecord leader, HashSet<HostRecord> replicationHolders)
    {
        System.out.println("In setTopicPartitionLeader already... ");
        System.out.println("=leader=");
        System.out.println(leader.toString());
        System.out.println("=followers=");
        for(HostRecord oneFollower : replicationHolders)
        {
            System.out.println(oneFollower.toString());
        }

    }


    public void getCoordinator(String groupId) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
//        while (!topics_coordinator.containsKey(groupId)) {
        TcpClient client = new TcpClient(defaultZookeeper.host,defaultZookeeper.port);
        List<Object> arguments = new ArrayList<>();
        arguments.add(groupId);
        Message request = new Message(MessageType.GET_COORDINATOR, arguments);
        client.setHandler(this, request);
        client.run();

//        HostRecord coordinator = topics_coordinator.get(groupId);
//        List<Object> arguments = new ArrayList();
//        arguments.add(coordinator);
//        Message response = new Message(MessageType.UPDATE_COORDINATOR, arguments);

    }
    //    public Message topicAss
    public void updateTopicPartitionLeader(String topic, List<Pair<Integer,Broker>> partitionLeaders) {
        topicPartitionLeaders.put(topic, partitionLeaders);
    }

    public void update(String s) {
        System.out.println("received response from broker");
        return;
    }
    public void listen() throws IOException, ClassNotFoundException, InterruptedException {

        listenSocket.listen();
//         while (true) {
//            // Send hearbeat per 1 min
//            Thread.sleep(updateClusterInterval);
//            try {
//                updateCluster();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InvocationTargetException e) {
//                e.printStackTrace();
//            } catch (NoSuchMethodException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (IllegalAccessException e) {
//                e.printStackTrace();
//            }
//        }
    }

    public static void main(String argv[]) throws Exception {
        brokerTest p = new brokerTest("localhost", 9008, "localhost", 2181);
        p.listen();
        p.registerToZookeeper();
//        p.getTopicConsumer();
        p.getTopic();
//        p.getCoordinator("1111");
//        p.registerToZookeeper();
//
//        p.sendMessage("topic1", "1");
//        //sleep(1000);
//        p.sendMessage("topic2", "2");
//        //sleep(1000);
//        p.sendMessage("topic3", "3");
        //sleep(1000);


    }
}

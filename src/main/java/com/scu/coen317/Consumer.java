package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;


public class Consumer {
    String host;
    int port;
    String groupId;
    TcpServer serverSocket;
    HostRecord coordinator;
    TcpServerEventHandler serverHandler;
    TcpClientEventHandler consumerClientEventHandler;

    // default brokers and broker cache
    HostRecord defaultBroker;
    List<HostRecord> brokers;

    Map<String, List<Pair<Integer, HostRecord>>> subscribedTopicPartitions;


    // for leader of group
//    boolean isLeader;
    Map<String, Consumer> groupTopicAndConsumer;
    Map<String, List<Pair<Integer, Broker>>> groupTopicAndPartition;


    public void setToLeader() {
        serverSocket = new TcpServer(port);
//        setHandler();
    }

    public Consumer (String host, int port, String groupId, String defaultBrokerIp, int defaultBrokerPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.host = host;
        this.port = port;
        this.groupId = groupId;

        // ask default broker this group's coordinator (broker)
        this.defaultBroker = new HostRecord(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(this.defaultBroker);
    }


    public void subscribe(String topic) throws IOException {
        if (subscribedTopicPartitions.containsKey(topic)) {
            return;
        }
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(this.groupId);

        Message request = new Message(MessageType.SUBSCRIBE_TOPIC, arguments);
        // send to coordinator and wait for partitions of this topic
        TcpClient consumerClient = new TcpClient(coordinator.getHost(), coordinator.getPort());
        consumerClient.setHandler(this, request);
    }

    public void assignByRebalancePlan(Map<String, List<Pair<Integer, HostRecord>>> topicPartitions) {
        subscribedTopicPartitions = topicPartitions;
    }

    public void rebalance() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        TcpClient client = new TcpClient(coordinator.getHost(), coordinator.getPort());
        List<Object> arguments = new ArrayList<>();
        arguments.add(this.groupId);
        Message request = new Message(MessageType.REBALANCE, arguments);
        client.setHandler(this,request);
        client.run();
    }

    public List<ConsumerRecord> poll() {

        // multicast of each partition in subscribedPartitions;
        while (true) {
            // new Thread接收回传讯息
        }
    }


    void pickBroker() throws IOException {
        if (brokers.size() != 0) {
            defaultBroker = brokers.get(0);
        } else {

        }
    }

    public void receiveConsumerJoinGroupRegistrationAck(String ack) {
        System.out.println(ack);
    }


    public void findCoordinator(HostRecord broker) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Message request = new Message(MessageType.CREATE_TOPIC.FIND_COORDINATOR, Collections.singletonList(this.groupId));
        // send request to defaultBroker with the groupId
        TcpClient sock = new TcpClient(broker.getHost(), broker.getPort());
        sock.setHandler(this, request);
        sock.run();
    }

    public void updateCoordinator(HostRecord coordinator) {
        this.coordinator = coordinator;
        System.out.println("My coordinator is " + coordinator.getPort());
    }

    // to coordinator
    public void sendHeartBeat() {

    }

    public static void main(String[] args) {
        Consumer consumerSend = null;
        try {
            consumerSend = new Consumer("localhost", 10001, "group1", "localhost", 9005);
            consumerSend.findCoordinator(consumerSend.defaultBroker);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

    }
}

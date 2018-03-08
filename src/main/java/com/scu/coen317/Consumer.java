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
        consumerClient.setHandler(this.getClass(), this, request);
    }

    public void assignByRebalancePlan(Map<String, List<Pair<Integer, HostRecord>>> topicPartitions) {
        subscribedTopicPartitions = topicPartitions;
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

    public void findCoordinator(HostRecord broker) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Message request = new Message(MessageType.CREATE_TOPIC.FIND_COORDINATOR, Collections.singletonList(this.groupId));
        // send request to defaultBroker with the groupId
        TcpClient sock = new TcpClient(broker.getHost(), broker.getPort());
        sock.setHandler(this.getClass(), this, request);
        sock.run();
    }

    public void updateCoordinator(HostRecord coordinator) {
        this.coordinator = coordinator;
        System.out.println("coordinator's host ： " + coordinator.getHost());
        System.out.println("coordinator's port ： " + coordinator.getPort());
//        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK);
//        return response;
    }

    // to coordinator
    public void sendHeartBeat() {

    }

    public static void main(String[] args) {
        Consumer consumerSend = null;
        try {
            consumerSend = new Consumer("localhost", 10001, "group1", "localhost", 9000);
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

package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.util.*;

public class Consumer {
    String ip;
    int port;
    String groupId;
    TcpServer serverSocket;
    Broker coordinator;
    TcpClientEventHandler consumerClientEventHandler;

    // default brokers and broker cache
    Broker defaultBroker;
    List<Broker> brokers;

    Map<String, Pair<Integer, Broker>> subscribedTopicPartitions;


    // for leader of group
//    boolean isLeader;
    Map<String, Consumer> groupTopicAndConsumer;
    Map<String, List<Pair<Integer, Broker>>> groupTopicAndPartition;


    public void setToLeader() {
        serverSocket = new TcpServer(port);
        //serverSocket.addEventHandler( new TcpServerEventHandler());
        serverSocket.listen();
    }

    public Consumer (String ip, int port, String groupId, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.ip = ip;
        this.port = port;
        this.groupId = groupId;

        // ask default broker this group's coordinator (broker)
        defaultBroker = new Broker(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(defaultBroker);
        setHandler();
        findCoordinator(defaultBroker);
    }

    public void setHandler() {
        //this.consumerClientEventHandler = new TcpClientEventHandler(this);
    }

    public void subscribe(String topic) throws IOException {
        if (subscribedTopicPartitions.containsKey(topic)) {
            return;
        }

        // send to coordinator and wait for patitions of this topic
        TcpClient consumerClient = new TcpClient(coordinator.host, coordinator.port);
        consumerClient.addEventHandler(consumerClientEventHandler);
        consumerClient.connect();
        consumerClient.send(Collections.singletonList("test"));
    }

    public List<ConsumerRecord> poll() {

        // multicast of each partition in subscribedPartitions;
        while (true) {
            // new Thread接收回传讯息
        }
    }


    Broker pickBroker() throws IOException {
        Broker broker = null;
        if (brokers.size() != 0) {
            broker = brokers.get(0);
        } else {

        }
        return broker;
    }

    public void findCoordinator(Broker defaultBroker) throws IOException {

        // send request to defaultBroker with the groupId
        TcpClient sock = new TcpClient(defaultBroker.host, defaultBroker.port);
        sock.addEventHandler(consumerClientEventHandler);
        sock.connect();
        sock.send(Collections.singletonList("1"));
    }

    // to coordinator
    public void sendHeartBeat() {

    }
}

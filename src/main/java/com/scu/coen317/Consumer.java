package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Consumer {
    String ip;
    int port;
    String groupId;
    TcpServer serverSocket;
    Broker coordinator;

    // default brokers and broker cache
    Broker defaultBroker;
    List<Broker> brokers;

    Map<String, Pair<Integer, Broker>> subscribedTopicPartitions;


    // for leader of group
    boolean isLeader;
    Map<String, Consumer> groupTopicAndConsumer;
    Map<String, List<Pair<Integer, Broker>>> groupTopicAndPartition;




    public Consumer (String ip, int port, String groupId, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.ip = ip;
        this.port = port;
        this.groupId = groupId;

        // ask default broker this group's coordinator (broker)
        defaultBroker = new Broker(defaultBrokerIp, defaultBrokerPort);
        brokers.add(defaultBroker);

        coordinator = findCoordinator(defaultBroker);

        if (isLeader) {
            serverSocket = new TcpServer(port);
            serverSocket.addEventHandler(new ConsumerServerEventHandler(this));
            serverSocket.listen();
        }
    }

    public void subscribre(String topic) throws IOException {
        if (subscribedTopicPartitions.containsKey(topic)) {
            return;
        }

        // send to coordinator and wait for patitions of this topic
        TcpClient consumerClient = new TcpClient(coordinator.ip, coordinator.port);
        consumerClient.addEventHandler(new ConsumerClientEventHandler());
        List<Object> request = null;
        consumerClient.send(request);
//        List<Pair<Integer, Broker>> partitions = findSubscribedPartition(coordinator);
        // update subscribedTopicPartitions
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

    public Broker findCoordinator(Broker defaultBroker) throws IOException {
        Broker coordinator = null;

        // send request to defaultBroker with the groupId
        TcpClient sock = new TcpClient("localhost", 5000);
        return coordinator;
    }

    // to coordinator
    public void sendHeartBeat() {

    }

    public Map<String, List<Pair<Integer, Broker>>> rebalance(String groupId) {
        // calculate alive consumer in the group

        // rebalance
        Map<String, List<Pair<Integer, Broker>>> balanceMap = new HashMap();
        return balanceMap;
    }

    public List<Pair<Integer, Broker>> findSubscribedPartition(Broker coordinator) {
        List<Pair<Integer, Broker>> partitions = null;


        return partitions;
    }
}

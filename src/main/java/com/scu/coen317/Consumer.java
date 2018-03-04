package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

public class Consumer {
    String ip;
    int port;
    String groupId;
    ServerSocket receiveSocket;
    Broker coordinator;

    // default brokers and broker cache
    List<Broker> brokers;

    Map<String, Pair<Integer, Broker>> subscribedTopicPartitions;


    // for leader of group
    Map<String, Consumer> groupTopicAndConsumer;
    Map<String, List<Pair<Integer, Broker>>> groupTopicAndPartition;




    public Consumer (String ip, int port, String groupId) throws IOException {
        this.ip = ip;
        this.port = port;
        this.groupId = groupId;

        // ask default broker this group's coordinator (broker)
        Broker defaultBroker = pickBroker();

        coordinator = findCoordinator(defaultBroker);
        receiveSocket = new ServerSocket(port);
    }

    public void subscribre(String topic) throws IOException {
        if (subscribedTopicPartitions.containsKey(topic)) {
            return;
        }




        List<Pair<Integer, Broker>> partitions = findSubscribedPartition(coordinator);
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
        }
        return broker;
    }

    public Broker findCoordinator(Broker defaultBroker) throws IOException {
        Broker coordinator = null;

        // send request to defaultBroker with the groupId
        return coordinator;
    }

    // to coordinator
    public void sendHeartBeat() {

    }

    public Map<String, List<Pair<Integer, Broker>>> rebalance(String groupId) {
        // calculate alive consumer in the group

        // rebalance
    }

    public List<Pair<Integer, Broker>> findSubscribedPartition(Broker coordinator) {
        List<Pair<Integer, Broker>> partitions = null;


        return partitions;
    }
}

package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class Zookeeper {
    String host;
    int port;
    ServerSocket receiveSocket;

    // min heap round robin timestamp queue
    // assign brokers when new producer apply a new topic
    PriorityQueue<Pair<Timestamp,Broker>> clusters;

    Map<String, Pair<Integer,Broker>> topic_map;

    // 接收來自producer的create_topic
    // 回傳這個topic, partition的負責人給傳的那個人
    public Zookeeper(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        receiveSocket = new ServerSocket(port);
        clusters = new PriorityQueue<>((p1, p2) -> p1.getKey().compareTo(p2.getKey()));
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
}

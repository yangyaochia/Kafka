package com.scu.coen317;

import javafx.util.Pair;

import java.util.List;

public class Consumer {
    String ip;
    int port;
    String groupId;
    List<Broker> brokers;
    List<Pair<Integer, Broker>> subscribedPartitions;
    List<Consumer> group;

    public Consumer (String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
}

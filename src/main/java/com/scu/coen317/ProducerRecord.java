package com.scu.coen317;

import javafx.util.Pair;

public class ProducerRecord {
    String topic;

    //int partition;
    Pair<Integer, Broker> partition;

    //K key;
    String content;

    public ProducerRecord(String topic, Pair<Integer, Broker> partition, String content) {
        this.topic = topic;
        this.partition = partition;
        this.content = content;
    }
}

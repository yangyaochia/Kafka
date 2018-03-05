package com.scu.coen317;

public class Topic implements java.io.Serializable {
    String name;
    int key;
    int partition = 1;
    int replication = 1;

    public Topic(String name) {
        this.name = name;
    }
    public Topic(String name, int partition, int replication) {
        this.name = name;
        this.partition = partition;
        this.replication = replication;
    }
    public String getName() {
        return this.name;
    }
}

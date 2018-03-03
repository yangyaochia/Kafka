package com.scu.coen317;

public class Topic implements java.io.Serializable {
    private String name;
    private int key;
    private int partition = 1;
    private int replication = 1;


    public Topic(String name) {
        this.name = name;
    }

    public Topic(String name, int partition, int replication) {
        this.name = name;
        this.partition = partition;
        this.replication = replication;
    }
}

package com.scu.coen317;

import java.io.Serializable;

public class Topic implements Serializable {
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

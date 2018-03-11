package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class TestBroker {


    public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        Broker broker = new Broker("localhost", 9005, "localhost", 2181);
        broker.registerToZookeeper();

//        Map<Integer,HostRecord> partitions1 = new HashMap<>();
//        partitions1.put(1, broker.thisHost);
//        partitions1.put(2, broker.thisHost);
//        partitions1.put(3, broker.thisHost);
//        partitions1.put(4, broker.thisHost);
//        broker.topicsPartitionLeaderCache.put("topic1", partitions1);
//        Map<Integer,HostRecord> partitions2 = new HashMap<>();
//        partitions2.put(1, broker.thisHost);
//        partitions2.put(2, broker.thisHost);
//        broker.topicsPartitionLeaderCache.put("topic2", partitions2);


//        Map<String, HostRecord> topics_coordinator = new HashMap<>();
//        topics_coordinator.put("group1", broker.thisHost);
//        broker.topics_coordinator = topics_coordinator;

//        broker.consumerLeader.put("group1", new HostRecord("localhost", 10001));

//        broker.updateCoordinator("group1", broker.thisHost);
        broker.listen();
    }
}

package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class TestZookeeper {
    public static void main(String[] args) throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, ClassNotFoundException {
        Zookeeper z = new Zookeeper("localhost", 2181);
        HashMap<Integer,HostRecord> partitions = new HashMap<>();
        HostRecord broker = new HostRecord("localhost", 9005);
        partitions.put(1, broker);
        partitions.put(2, broker);
        //partitions.put(3, broker);
        //partitions.put(4, broker);
        z.topicAssignmentHash.put("topic1", partitions);
//        z.coordinatorAssignmentHash.put("group1", broker);
        z.listen();
    }
}

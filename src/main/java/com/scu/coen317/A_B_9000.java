package com.scu.coen317;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class A_B_9000 {

    public static void main(String argv[]) throws Exception {
//        Set<HostRecord> replicationHolders = new HashSet<>();
//        HostRecord b1h = new HostRecord("localhost", 9000);
//        HostRecord b2h = new HostRecord("localhost", 9001);
//        HostRecord b3h = new HostRecord("localhost", 9002);
////
//        replicationHolders.add(b2h);
//        replicationHolders.add(b3h);
        Broker b1 = new Broker("localhost", 9000, "localhost", 2181);
        //b1.setTopicPartitionLeader("topic1", 0, new HostRecord("localhost", 9000), (HashSet<HostRecord>) replicationHolders);
        b1.registerToZookeeper();
        b1.listen();
        b1.sendHeartBeat();


//        List<Object> argument = new ArrayList<>();
//        Message response;
//        Map<Integer,HostRecord> leaders = new HashMap<>();
//        leaders.put(0, new HostRecord("localhost", 9000));
//        leaders.put(1, new HostRecord("localhost", 9000));
//        topicsPartitionLeader.put(topicName, leaders);

        // This broker does now know the topic, then ask the zookeeper
////        if ( !topicsPartitionLeader.containsKey(topicName) ) {
//        Topic topic = new Topic("hahahaha");
//        topic.partition = 3;
//        topic.replication = 1;
////        HostRecord temp = new HostRecord(b1h.getHost(), b1h.getPort());
//        argument.add(topic);
//        argument.add(b1h);
//        b1.getTopic(topic, new HostRecord("localhost", 8000));

    }
}

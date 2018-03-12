package com.scu.coen317;

public class A_B_9005 {
    public static void main(String argv[]) throws Exception {
//        Set<HostRecord> replicationHolders = new HashSet<>();
//        HostRecord b1h = new HostRecord("localhost", 9000);
//        HostRecord b2h = new HostRecord("localhost", 9001);
//        HostRecord b3h = new HostRecord("localhost", 9002);
////
//        replicationHolders.add(b2h);
//        replicationHolders.add(b3h);
        Broker b1 = new Broker("localhost", 9005, "localhost", 2181);
        //b1.setTopicPartitionLeader("topic1", 0, new HostRecord("localhost", 9000), (HashSet<HostRecord>) replicationHolders);
        b1.registerToZookeeper();
        b1.listen();
        b1.sendHeartBeat();
    }
}

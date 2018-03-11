package com.scu.coen317;

import java.util.HashSet;
import java.util.Set;

public class BrokerTestYaoChia {
    public static void main(String argv[]) throws Exception {

//        HostRecord z = new HostRecord("localhost", 2181);
//        Broker b1 = new Broker("localhost", 9000, "localhost", 2181);
//        Set<HostRecord> replicationHolders = new HashSet<>();
//        b1.setTopicPartitionLeader("topic1", 0, new HostRecord("localhost", 9000), (HashSet<HostRecord>) replicationHolders);
//        b1.registerToZookeeper();
//        b1.listen();
//        Topic t = new Topic("topic1",1,1);
//
//
        Broker b2 = new Broker("localhost", 9001, "localhost", 2181);
////        b2.registerToZookeeper();
        b2.listen();
//
        Broker b3 = new Broker("localhost", 9002, "localhost", 2181);
//        b3.registerToZookeeper();
        b3.listen();
//
//        b1.getTopic(t);
//
//        Broker b2 = new Broker("localhost", 9001, "localhost", 2181);
//
//        b2.listen();
//        b3.listen();
//
//        HostRecord b1h = new HostRecord("localhost", 9000);
//        HostRecord b2h = new HostRecord("localhost", 9001);
//        HostRecord b3h = new HostRecord("localhost", 9002);
//
//        Set<HostRecord> replicationHolders2 = new HashSet<>();
//        replicationHolders2.add(b1h);
//        replicationHolders2.add(b3h);
//        b2.setTopicPartitionLeader("topic1", 1, new HostRecord("localhost", 9001), (HashSet<HostRecord>) replicationHolders2);


//        Broker xinzhuBroker = new Broker("localhost", 9005, "localhost", 2181);
//        xinzhuBroker.updateCoordinator("group1", new HostRecord(xinzhuBroker.host, xinzhuBroker.port));
////        xinzhuBroker.listen();
//        Broker xinzhuBroker = new Broker("localhost", 9005, "localhost", 2181);
//        xinzhuBroker.updateCoordinator("group1", xinzhuBroker.thisHost);
//        xinzhuBroker.listen();
    }
}

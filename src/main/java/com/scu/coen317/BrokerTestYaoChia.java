package com.scu.coen317;

import java.util.HashSet;
import java.util.Set;

public class BrokerTestYaoChia {
    public static void main(String argv[]) throws Exception {

        Broker b2 = new Broker("localhost", 9001, "localhost", 2181);
        Broker b3 = new Broker("localhost", 9002, "localhost", 2181);
        b2.listen();
        b3.listen();

        Set<HostRecord> replicationHolders = new HashSet<>();
        replicationHolders.add(new HostRecord("localhost", 9001));
        replicationHolders.add(new HostRecord("localhost", 9002));
        Broker b1 = new Broker("localhost", 9000, "localhost", 2181);
        b1.setTopicPartitionLeader("topic1", 0, new HostRecord("localhost", 9000), (HashSet<HostRecord>) replicationHolders);
        b1.setTopicPartitionLeader("topic1", 1, new HostRecord("localhost", 9000), (HashSet<HostRecord>) replicationHolders);
        b1.listen();


//        Broker xinzhuBroker = new Broker("localhost", 9005, "localhost", 2181);
//        xinzhuBroker.updateCoordinator("group1", new HostRecord(xinzhuBroker.host, xinzhuBroker.port));
////        xinzhuBroker.listen();
//        Broker xinzhuBroker = new Broker("localhost", 9005, "localhost", 2181);
//        xinzhuBroker.updateCoordinator("group1", xinzhuBroker.thisHost);
//        xinzhuBroker.listen();
    }
}

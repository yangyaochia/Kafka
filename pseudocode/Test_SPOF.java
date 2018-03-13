package com.scu.coen317;

public class Test_SPOF {

    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 2181);
        z.listen();
        z.monitorCluster();

        Broker b1 = new Broker("localhost", 9000, "localhost", 2181);
        b1.registerToZookeeper();
        b1.listen();
        b1.sendHeartBeat();

        Broker b2 = new Broker("localhost", 9001, "localhost", 2181);
        b2.listen();
        b2.registerToZookeeper();
        b2.sendHeartBeat();

        Broker b3 = new Broker("localhost", 9002, "localhost", 2181);
        b3.listen();
        b3.registerToZookeeper();
        b3.sendHeartBeat();

        Producer p = new Producer("localhost", 8000, "localhost", 9000);
        p.createTopic("Distributed System Topic", 3,3);
        for ( int i = 0 ; i < 10 ; i++) {
            p.publishMessage("Distributed System Topic", "DS Topic: Message from P1 " + Integer.toString(i));
        }

//        b1.


    }
}

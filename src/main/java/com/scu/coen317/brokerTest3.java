package com.scu.coen317;

public class brokerTest3 {

    public static void main(String argv[]) throws Exception {
//        brokerTest p = new brokerTest("localhost", 9008, "localhost", 2181);
        brokerTest p2 = new brokerTest("localhost", 9006, "localhost", 2181);
//        brokerTest p3 = new brokerTest("localhost", 9006, "localhost", 2181);
//        brokerTest p3 = new brokerTest("localhost", 9006, "localhost", 2181);
//        p.listen();
        p2.listen();
//        p3.listen();
//        p.registerToZookeeper();
        p2.registerToZookeeper();

        p2.sendHeartBeat();
//        p3.registerToZookeeper();
//        p.getTopicConsumer();
//        p.getTopic();

//        sleep(5000);
//        p.TestReassign();

//
//        p.getCoordinator("1111");
//        p.registerToZookeeper();
//
//        p.sendMessage("topic1", "1");
//        //sleep(1000);
//        p.sendMessage("topic2", "2");
//        //sleep(1000);
//        p.sendMessage("topic3", "3");
        //sleep(1000);


    }
}

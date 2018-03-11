package com.scu.coen317;

import java.util.ArrayList;
import java.util.List;

public class ProducerTest {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8000, "localhost", 9001);
//        p.addDefaultBroker("localhost", 9001);
        p.createTopic("topic1", 1,1);
//        p.createTopic("topic2", 2,2);
        p.publishMessage("topic1", "test1");
        p.publishMessage("topic1", "test2");
//        p.publishMessage("topic2", "test3");
        p.publishMessage("topic1", "test3");
//
        p.publishMessage("topic1", "test4");
        p.publishMessage("topic1", "test5");
        p.publishMessage("topic1", "test6");
        //sleep(1000);
//        p.publishMessage("topic2", "2");
//        //sleep(1000);
//        p.publishMessage("topic3", "3");
        //sleep(1000);
    }
}

package com.scu.coen317;

public class ProducerTest {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8000, "localhost", 9000);
        p.createTopic("topic1", 2,2);
        p.publishMessage("topic1", "test1");
        //p.publishMessage("topic1", "test2");
//        //sleep(1000);
//        p.publishMessage("topic2", "2");
//        //sleep(1000);
//        p.publishMessage("topic3", "3");
        //sleep(1000);
    }
}

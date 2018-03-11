package com.scu.coen317;

import java.util.ArrayList;
import java.util.List;

public class ProducerTest {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8000, "localhost", 9000);

//        p.printDefaultBrokerList();
//
//        p.createTopic("Distributed System Topic", 2,1);
//        p.createTopic("Santa Clara Univ Topic", 3,1);
//
//        p.printDefaultBrokerList();

        for ( int i = 0 ; i < 2 ; i++) {
            p.publishMessage("Distributed System Topic", "Message from P1 " + Integer.toString(i));
        }
//        for ( int i = 0 ; i < 25 ; i++) {
//            p.publishMessage("Santa Clara Univ Topic", "Message from P1 " + Integer.toString(i));
//        }


//        p.createTopic("topic2", 1,1);
//        for ( int i = 0 ; i < 25 ; i++) {
//            p.publishMessage("Empty Topic", "Message from P1 " + Integer.toString(i));
//        }
//

//        p.publishMessage("topic1", "test1");
//        p.publishMessage("topic1", "test2");
//        p.publishMessage("topic2", "test3");
//        p.publishMessage("topic1", "test3");
//
//        p.publishMessage("topic1", "test4");
        //sleep(1000);
//        p.publishMessage("topic2", "2");
//        //sleep(1000);
//        p.publishMessage("topic3", "3");
        //sleep(1000);
    }
}

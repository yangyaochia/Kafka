package com.scu.coen317;

import java.util.ArrayList;
import java.util.List;

public class A_P_8000 {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8000, "localhost", 9000);
        p.addDefaultBroker("localhost",9001);
//        p.printDefaultBrokerList();
////
        p.createTopic("Distributed System Topic", 1,3);
//        p.createTopic("Santa Clara Univ Topic", 1,2);
////
//        p.printDefaultBrokerList();
//
        for ( int i = 0 ; i < 1500 ; i++) {
            p.publishMessage("Distributed System Topic", "DS Topic: Message from P1 " + Integer.toString(i));
        }
//        for ( int i = 25 ; i < 50 ; i++) {
//            p.publishMessage("Santa Clara Univ Topic", "SCU Topic: Message from P1 " + Integer.toString(i));
//        }

//        for ( int i = 25 ; i < 50 ; i++) {
//            p.publishMessage("Distributed System Topic", "DS Topic: Message from P1 " + Integer.toString(i));
//        }
//        for ( int i = 0 ; i < 25 ; i++) {
//            p.publishMessage("Santa Clara Univ Topic", "SCU Topic: Message from P1 " + Integer.toString(i));
//        }
//        p.createTopic("SPOF Test", 1,2);
//        for ( int i = 0 ; i < 25 ; i++) {
//            p.publishMessage("SPOF Test", "SPOF Topic: Message from P1 " + Integer.toString(i));
//        }

//        for ( int i = 25 ; i < 50 ; i++) {
//            p.publishMessage("SPOF Test", "SPOF Topic: Message from P1 " + Integer.toString(i));
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

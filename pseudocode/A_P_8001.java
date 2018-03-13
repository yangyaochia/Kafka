package com.scu.coen317;

public class A_P_8001 {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8001, "localhost", 9001);
//        p.addDefaultBroker("localhost",9001);
//        p.printDefaultBrokerList();
////
        p.createTopic("SCU Topic", 2, 1);
//        p.createTopic("Santa Clara Univ Topic", 1,2);
////
//        p.printDefaultBrokerList();
//
        for (int i = 0; i < 1500; i++) {
            p.publishMessage("SCU Topic", "SCU Topic: Message from 8001 " + Integer.toString(i));
        }
    }



}

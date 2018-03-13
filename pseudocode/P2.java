package com.scu.coen317;

public class P2 {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8001, "localhost", 9001);
//        p.publishMessage("Distributed System Topic", "Message from P2 " + Integer.toString(0));
        for ( int i = 0 ; i < 25 ; i++) {
            p.publishMessage("Distributed System Topic", "Message from P2 " + Integer.toString(i));
        }
//        for ( int i = 0 ; i < 25 ; i++) {
//            p.publishMessage("Santa Clara Univ Topic", "Message from P2 " + Integer.toString(i));
//        }
    }
}

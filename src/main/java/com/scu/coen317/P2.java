package com.scu.coen317;

public class P2 {
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 8001, "localhost", 9000);
        for ( int i = 0 ; i < 25 ; i++) {
            p.publishMessage("topic1", "Message from P2 " + Integer.toString(i));
        }
    }
}

package com.scu.coen317;

public class A_B_9005 {
    public static void main(String argv[]) throws Exception {
        Broker b = new Broker("localhost", 9005, "localhost", 2181);
        b.listen();
        b.registerToZookeeper();
        b.sendHeartBeat();
    }
}

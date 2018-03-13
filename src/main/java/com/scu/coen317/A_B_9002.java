package com.scu.coen317;

public class A_B_9002 {

    public static void main(String argv[]) throws Exception {
        Broker b3 = new Broker("localhost", 9002, "localhost", 2181);
        b3.listen();
        b3.registerToZookeeper();
        b3.sendHeartBeat();
    }
}

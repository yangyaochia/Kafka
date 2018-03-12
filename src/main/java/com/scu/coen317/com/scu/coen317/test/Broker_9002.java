package com.scu.coen317.com.scu.coen317.test;

import com.scu.coen317.Broker;

public class Broker_9002 {

    public static void main(String argv[]) throws Exception {
        Broker b3 = new Broker("localhost", 9002, "localhost", 2181);
        b3.listen();
        b3.registerToZookeeper();
//        b3.sendHeartBeat();
    }
}

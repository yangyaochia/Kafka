package com.scu.coen317;

import java.util.HashSet;
import java.util.Set;

public class A_B_9001 {
    public static void main(String argv[]) throws Exception {

        Broker b2 = new Broker("localhost", 9001, "localhost", 2181);
        b2.listen();
        b2.registerToZookeeper();
        b2.sendHeartBeat();
    }
}

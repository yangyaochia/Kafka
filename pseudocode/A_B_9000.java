package com.scu.coen317;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class A_B_9000 {

    public static void main(String argv[]) throws Exception {

        Broker b1 = new Broker("localhost", 9000, "localhost", 2181);
        b1.registerToZookeeper();
        b1.listen();
        b1.sendHeartBeat();

    }
}

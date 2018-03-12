package com.scu.coen317.com.scu.coen317.test;

import com.scu.coen317.Zookeeper;

public class Zookeeper_2181 {
    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 2181);
        z.listen();
    }
}

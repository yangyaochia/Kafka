package com.scu.coen317;

public class ZookeeperTest {
    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 2181);
        z.listen();
    }
}

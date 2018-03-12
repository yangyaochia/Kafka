package com.scu.coen317;

public class A_Z {
    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 2181);
//        z.coordinatorAssignmentHash.put("group1", new HostRecord("localhost", 9002));
        z.listen();
        z.monitorCluster();
    }
}

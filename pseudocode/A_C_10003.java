package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class A_C_10003 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con3 = new Consumer("localhost", 10003, "group2",
                "localhost", 9000);
        con3.findCoordinator();
//        con1.updateCoordinator(new HostRecord("localhost", 9005));
        con3.joinToGroup();
        con3.subscribe("Santa Clara University Topic");
//        con3.subscribe("Santa Clara Univ Topic");
//        Thread.sleep(1000);
        con3.poll();
    }
}

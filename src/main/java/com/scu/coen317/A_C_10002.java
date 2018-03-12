package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class A_C_10002 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con2 = new Consumer("localhost", 10002, "group1",
                "localhost", 9000);
        con2.findCoordinator();
//        con2.updateCoordinator(new HostRecord("localhost", 9000));
        con2.joinToGroup();
        con2.subscribe("Distributed System Topic");
//        con2.subscribe("Santa Clara Univ Topic");
        Thread.sleep(1000);
        con2.poll();
    }

}

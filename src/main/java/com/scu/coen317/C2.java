package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class C2 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con2 = new Consumer("localhost", 10002, "group1", "localhost", 9000);
        con2.findCoordinator();
//        con1.updateCoordinator(new HostRecord("localhost", 9005));
        con2.joinToGroup();
        con2.subscribe("Empty Topic");
        Thread.sleep(3000);
        con2.poll();
    }
}

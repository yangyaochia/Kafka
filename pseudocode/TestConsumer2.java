package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TestConsumer2 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con2 = new Consumer("localhost", 10002,
                "group1",
                "localhost", 9000);
        con2.findCoordinator();
        con2.joinToGroup();
        con2.subscribe("Distributed System Topic");
//        con2.subscribe("topic2");
        con2.poll();
    }
}
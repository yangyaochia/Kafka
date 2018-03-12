package com.scu.coen317.com.scu.coen317.test;

import com.scu.coen317.Consumer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class Consumer_10004 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con2 = new Consumer("localhost", 10004,
                "group1",
                "localhost", 9000);
        con2.joinToGroup();
        con2.subscribe("topic1");
//        con2.subscribe("topic2");
        con2.poll();
    }
}
package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class TestZookeeper {
    public static void main(String[] args) throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException, ClassNotFoundException {
        Zookeeper z = new Zookeeper("localhost", 2181);
        z.listen();
    }
}

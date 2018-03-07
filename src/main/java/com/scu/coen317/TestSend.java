package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

public class TestSend {
    public static void main(String[] args) throws IOException {
        try {
            Consumer consumerSend = new Consumer("localhost", 9000, "group1", "localhost", 9001);

//            consumerSend.subscribe("testTopic");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

}

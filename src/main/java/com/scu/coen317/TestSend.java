package com.scu.coen317;

import java.io.IOException;

public class TestSend {
    public static void main(String[] args) throws IOException {
        try {
            Consumer consumer = new Consumer("localhost", 8888, "group1", "localhost", 8800);
            consumer.findCoordinator();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

package com.scu.coen317;

import java.io.IOException;
import java.util.Collections;

public class TestSend {
    public static void main(String[] args) throws IOException {
        try {
            Consumer consumerSend = new Consumer("localhost", 9000, "group1", "localhost", 9001);

            consumerSend.subscribe("testTopic");
        } catch (IOException e) {
            e.printStackTrace();
        }
//
//        TcpClient clientTest = new TcpClient("localhost", 9000);
//        clientTest.send(Collections.singletonList("test"));
    }

}

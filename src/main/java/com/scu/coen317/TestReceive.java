package com.scu.coen317;

import java.io.IOException;

public class TestReceive {
    public static void main(String[] args) {
        try {
            Consumer consumerReceive = new Consumer("localhost", 9001, "group1", "localhost", 9000);
//            System.out.println("Before send : " + consumerReceive.helloWord);
//            System.out.println("After send : " + consumerReceive.helloWord);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        TcpServer serverTest = new TcpServer(9000);
//        serverTest.listen();
    }
}

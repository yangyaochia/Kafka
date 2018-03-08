package com.scu.coen317;

import java.io.IOException;

public class TestReceive {
    public static void main(String[] args) {
        try {
            Broker broker = new Broker( "localhost", 9001);
//            System.out.println("Before send : " + consumerReceive.helloWord);
//            System.out.println("After send : " + consumerReceive.helloWord);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        TcpServer serverTest = new TcpServer(9000);
//        serverTest.listen();
    }
}

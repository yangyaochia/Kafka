package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;

public class A_C_10001 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con1 = new Consumer("localhost", 10002, "group1",
                "localhost", 9001);
//        con1.findCoordinator();
//        con1.updateCoordinator(new HostRecord("localhost", 9000));
        con1.joinToGroup();
        con1.subscribe("Distributed System Topic");
        Thread.sleep(1000);
        con1.poll();


        /*Consumer consumer = new Consumer("localhost", 10001, "group1", "localhost", 9005);
        consumer.initialLeader();
        Message request = new Message(MessageType.TEST1);
//        // send request to defaultBroker with the groupId
        try {
            TcpClient sock = new TcpClient("localhost", 9004);
            sock.setHandler(consumer, request);
            sock.run();
        } catch (Exception e) {
//            e.printStackTrace();
            TcpClient sock = new TcpClient("localhost", 9005);
            sock.setHandler(consumer, request);
            sock.run();
        }
*/
    }
}

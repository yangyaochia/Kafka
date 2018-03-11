package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class TestConsumer1 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con1 = new Consumer("localhost", 10001,
                "group1", "localhost",
                9001);
        con1.findCoordinator();
//        con1.updateCoordinator(new HostRecord("localhost", 9005));
        con1.joinToGroup();
//        con1.subscribe("topic1");

        HashMap<String, Map<Integer, HostRecord>> map = new HashMap<>();
        map.put("topic1", new HashMap<>());
        map.get("topic1").put(1, new HostRecord("localhost", 9001));
        con1.updateTopicPartition(map);
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

package com.scu.coen317.com.scu.coen317.test;

import com.scu.coen317.Consumer;
import com.scu.coen317.HostRecord;
import com.scu.coen317.TcpClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Consumer_10003 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        Consumer con3 = new Consumer("localhost", 10003,
                "group1",
                "localhost", 9000);
//        con3.updateCoordinator(new HostRecord("localhost", 9005));
        con3.findCoordinator();
        con3.joinToGroup();
        con3.subscribe("Distributed System Topic");
        Thread.sleep(5000);
        con3.poll();

        /*
        TcpClient sendReplace = new TcpClient("localhost", 9005);
        HashMap<String, Map<Integer, Map<HostRecord, HostRecord>>> newInfo = new HashMap<>();
        HashMap<HostRecord, HostRecord> newPair = new HashMap<>();
        newPair.put(new HostRecord("localhost", 9005),new HostRecord("localhost", 9010));

        /*
        HashMap<Integer, Map<HostRecord, HostRecord>> newRecord = new HashMap<>();
        newRecord.put(1, newPair);
        newInfo.put("topic1", newRecord);
        List<Object> arguments = new ArrayList<>();
        arguments.add(newInfo);
        Message request = new Message(MessageType.REPLACE_BROKER, arguments);
        sendReplace.setHandler(con3,request);
        sendReplace.run();
        */



//        TcpClient returnNewPartition = new TcpClient("localhost", 9005);
//        List<Object> arguments = new ArrayList();
//        arguments.add("topic1");
//        HashMap<Integer,HostRecord> partitions = new HashMap<>();
//        HostRecord broker = new HostRecord("localhost", 9005);
////        partitions.put(1, broker);
////        partitions.put(2, broker);
//        arguments.add(partitions);
//        Message request = new Message(MessageType.RETURN_TOPIC_FOR_COORDINATOR, arguments);
//        returnNewPartition.setHandler(con3, request);
//        returnNewPartition.run();


    }
}

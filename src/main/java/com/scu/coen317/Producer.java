package com.scu.coen317;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.*;

import java.util.*;
import java.io.*;

import static java.lang.Thread.sleep;

public class Producer {
    String host;
    int port;
    //TcpClient sock;
    //TcpClientEventHandler handler;
    // default brokers and broker cache
    Broker defaultBroker;
    List<Broker> brokers;

    // topic, <partition, 負責的broker>
    Map<String, List<Pair<Integer,Broker>> > topicPartitionLeaders;

    public Producer (String host, int port, String defaultBrokerIp, int defaultBrokerPort) throws IOException {
        this.host = host;
        this.port = port;

        //sock.setReadInterval(5000);


        defaultBroker = new Broker(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(defaultBroker);

        topicPartitionLeaders = new HashMap<>();

    }

    private int hashCode(String msg) {
        int hash = 5381;
        int i = 0;
        while (i < msg.length()) {
            hash = ((hash << 5) + hash) + msg.charAt(i++); /* hash * 33 + c */
        }
        return hash;
    }

    public void sendMessage(String topic, String msg) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        //sock.connect();
//        Message request = new Message("find");
//
//        request.arguments.add(topic);
//        request.arguments.add(1);
//        Topic t = new Topic(topic);
//        request.add(t);
//        request.add(msg);

        TcpClient sock = new TcpClient(defaultBroker.host, defaultBroker.port);
        final TcpClient that_sock = sock;
        final Producer this_producer = this;
        sock.addEventHandler(new TcpClientEventHandler(){
            public void onMessage(Message message) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
                //handler.onMessage(cid, msg);

                Class<?>[] inputTypes = message.toArray();
                System.out.println(message.getMethodName());
                Class clazz = Producer.class;
                System.out.println("find class : " + clazz.getName());
                System.out.println("input types : " + inputTypes.toString());
                Method method = clazz.getMethod(message.getMethodName(), inputTypes);
                System.out.println("find method : " + method.getName());

                Object[] inputs = new Object[message.arguments.size()];
                for (int i = 0; i < inputs.length; i++) {
                    inputs[i] = message.getArguments().get(i);
                }
                System.out.println(inputs.toString());
                Message response = (Message) method.invoke(this_producer, inputs);

                if (response == null) {
                    that_sock.close();
                }
//                System.out.println("* <"+client_id+"> "+ message.getMethodName());
                //msg.add(0, "echo : <"+client_id+"> ");
//                that_server.getClient(client_id).send(message);
                System.out.println(message.getMethodName());
            }
            public void onOpen() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
                System.out.println("* socket connected");

                Message message = new Message("find");

                that_sock.send(message);
            }
            public void onClose(){
                //handler.onClose(cid);
            }
        });
//        List<Pair<Integer,Broker>> ls= topicPartitionLeaders.get(topic);
//        if ( topicPartitionLeaders.get(topic) == null ) {
//            // 先問default broker list
//
//        }
//        //int totalPartition = topic_partition_leaders.get(topic).size();
//        int partition = hashCode(msg) % 4;

        sock.run();
    }
    public void updateTopicPartitionLeader(String topic, List<Pair<Integer,Broker>> partitionLeaders) {
        topicPartitionLeaders.put(topic, partitionLeaders);
    }

    public void update(String s) {
        System.out.println("received response from broker");
        return;
    }

    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 9001, "localhost", 9000);
        p.sendMessage("topic1", "1");
        //sleep(1000);
        p.sendMessage("topic2", "2");
        //sleep(1000);
        p.sendMessage("topic3", "3");
        //sleep(1000);
    }
}
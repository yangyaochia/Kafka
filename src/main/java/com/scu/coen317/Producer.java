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



        List<Pair<Integer,Broker>> ls= topicPartitionLeaders.get(topic);
        int partition = hashCode(msg) % 4;
        // Check if this producer knows where to send
//        if ( topicPartitionLeaders.get(topic) == null ) {
//
//
//        } else { //
//
//        }
        List<Object> argument = new ArrayList<>();
        argument.add(topic);
        argument.add(msg);
        Message message = new Message(MessageType.PUBLISH_MESSAGE, argument);

        TcpClient sock = new TcpClient(defaultBroker.host, defaultBroker.port);
        sock.setReadInterval(1000);
        final TcpClient that_sock = sock;
        sock.addEventHandler(new TcpClientEventHandler(){
            public void onMessage(Message msg){
                //handler.onMessage(cid, msg);
                if ( msg.getMethodName() == MessageType.PUBLISH_MESSAGE_ACK ) {
                    System.out.println((String)msg.getMethodNameValue());
                    that_sock.close();
                } else {

                }
                //System.out.println((String)msg.getMethodNameValue());
            }

            public void onOpen() {
                System.out.println("* socket connected");
                that_sock.send(message);
            }
            public void onClose(){
                //handler.onClose(cid);
            }
        });
        sock.run();
    }

    public void publishMessageAck(String message) {
        System.out.println(message);


        return;
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
        p.sendMessage("topic2", "2");
        p.sendMessage("topic3", "3");
    }
}
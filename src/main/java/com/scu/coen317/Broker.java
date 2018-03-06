package com.scu.coen317;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;

enum MethodName {
    FIND("find");

    private final String name;
    MethodName(String s) {
        name = s;
    }

    String getName() {
        return name;
    }
}

public class Broker {
    String host;
    int port;
    TcpServer listenSocket;
    TcpServerEventHandler serverHandler;
    // 某topic, partition 的其他組員是誰
    Map<String, List<String>> topicMessage;
    Map<String, List<Broker>> topicsMember;

    // 作为coordinator要用到的讯息
    Map<String, Broker> topics_coordinator;

    // each topic's consumer group leader
    Map<String, Consumer> consumerLeader;

    // 记录consumer， offset
    Map<Consumer, Integer> consumerOffset;

    public Broker(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        //receiveSocket = new ServerSocket(port);

        this.listenSocket = new TcpServer(port);
        setHandler();
        listenSocket.addEventHandler(this.serverHandler);

        topicsMember = new HashMap();
        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();
        topicMessage = new HashMap<>();
    }


    private void setHandler() {
        final TcpServer that_server = listenSocket;
        final Broker this_broker = this;
        this.serverHandler = new TcpServerEventHandler(){
            public void onMessage(int client_id, Message message) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {

//                List<Object> arguments = new ArrayList<>();
//                arguments.add("most useful");
//                arguments.add(1);
//                Message message = new Message(MessageType.CREATE_TOPIC, arguments);

                Class<?>[] inputTypes = message.toArray();
                System.out.println(message.getMethodName());
                Class clazz = Broker.class;
                Method method = clazz.getMethod(message.getMethodName(), inputTypes);
                Object[] inputs = new Object[message.arguments.size()];
                for (int i = 0; i < inputs.length; i++) {
                    inputs[i] = message.getArguments().get(i);
                }
                Message response = (Message) method.invoke(this_broker, inputs);
                //Message msg = method.invoke(this_broker, inputs);
                //if ( msg != null ) {
                    System.out.println("* <"+client_id+"> "+ response.getMethodName());
                    //msg.add(0, "echo : <"+client_id+"> ");
                    that_server.getClient(client_id).send(response);
                //}


            }
            public void onAccept(int client_id){
                System.out.println("* <"+client_id+"> connection accepted");
                that_server.setReadInterval(100 + that_server.getClients().size()*10);
            }
            public void onClose(int client_id){
                System.out.println("* <"+client_id+"> closed");
            }
        };
    }

    public Message receivedMessage(String topic, String message) {
        System.out.println("Hello??" + "topic map's size is " + topicMessage.size());

        List<String> list = topicMessage.getOrDefault(topic, new ArrayList<>());
        list.add(message);
        topicMessage.put(topic, list);

        return sendMessageAck();
    }

    public Message sendMessageAck() {
        List<Object> arguments = new ArrayList<>();
        arguments.add("Successful");
        Message response = new Message(MessageType.SEND_MESSAGE_ACK, arguments);
        return response;
    }

    public Broker findBroker() {
        return this;
    }


    public void listen() throws IOException, ClassNotFoundException {

        listenSocket.listen();
    }

    public static void main(String argv[]) throws Exception {
        Broker b = new Broker("localhost", 9000);
        b.listen();

    }
}


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
    }


    private void setHandler() {
        final TcpServer that_server = listenSocket;
        final Broker this_broker = this;
        this.serverHandler = new TcpServerEventHandler(){
            public void onMessage(int client_id, List<Object> msg) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {

                Message message = new Message();
                message.name = MessageType.CREATE_TOPIC;
                message.arguments = new ArrayList<>();
                message.arguments.add("most useful");
                message.arguments.add(1);

                Class<?>[] inputTypes = message.toArray();
                System.out.println(message.getMethodName());
                Class clazz = Broker.class;
                Method method = clazz.getMethod(message.name.toString(), inputTypes);
                Object[] inputs = new Object[message.arguments.size()];
                for (int i = 0; i < inputs.length; i++) {
                    inputs[i] = message.getArguments().get(i);
                }
                method.invoke(this_broker, inputs);
                //Message msg = method.invoke(this_broker, inputs);
                //if ( msg != null ) {
                    System.out.println("* <"+client_id+"> "+ (String)msg.get(0));
                    //msg.add(0, "echo : <"+client_id+"> ");
                    that_server.getClient(client_id).send(msg);
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
    public void find(String t, Integer i) {
//        Message msg
//        System.out.println("This broker's port number :" + this.port);
//        if ( this broket 知道) {
//            msg = ;
//        } else {
//            msg = createTopic() // 這個broker開一個client 去問zookeeper
//        }
//        return msg;
//        Message message = new Message();
//        message.name = "find";
//        message.arguments = new ArrayList<>();
//        message.arguments.add("most useful");
//        message.arguments.add(1);

        return;
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


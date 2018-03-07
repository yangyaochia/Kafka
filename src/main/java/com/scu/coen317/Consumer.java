package com.scu.coen317;

import javafx.util.Pair;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;


public class Consumer {
    String host;
    int port;
    String groupId;
    TcpServer serverSocket;
    Pair<String, Integer> coordinator;
    TcpServerEventHandler serverHandler;
    TcpClientEventHandler consumerClientEventHandler;

    // default brokers and broker cache
    String defaultBrokerHost;
    int defaultBrokerPort;
    List<Pair<String, Integer>> brokers;

    Map<String, List<Pair<Integer, Broker>>> subscribedTopicPartitions;


    // for leader of group
//    boolean isLeader;
    Map<String, Consumer> groupTopicAndConsumer;
    Map<String, List<Pair<Integer, Broker>>> groupTopicAndPartition;


    public void setToLeader() {
        serverSocket = new TcpServer(port);
        setHandler();
        //serverSocket.addEventHandler( new TcpServerEventHandler());
        serverSocket.listen();
    }

    public Consumer (String host, int port, String groupId, String defaultBrokerIp, int defaultBrokerPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.host = host;
        this.port = port;
        this.groupId = groupId;

        // ask default broker this group's coordinator (broker)
        this.defaultBrokerHost = defaultBrokerIp;
        this.defaultBrokerPort = defaultBrokerPort;
        brokers = new ArrayList();
        brokers.add(new Pair<>(defaultBrokerHost, defaultBrokerPort));

        findCoordinator(this.defaultBrokerHost, this.defaultBrokerPort);
    }

    public void setHandler() {
        final TcpServer that_server = serverSocket;
        final Consumer this_consumer = this;
        this.serverHandler = new TcpServerEventHandler(){
            public void onMessage(int client_id, Message message) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {

                Class<?>[] inputTypes = message.getInputParameterType();
                Class clazz = Broker.class;
                Method method = clazz.getMethod(message.methodName.toString(), inputTypes);
                Object[] inputs = message.getInputValue();
                method.invoke(this_consumer, inputs);


                System.out.println("* <"+client_id+"> ");
                //msg.add(0, "echo : <"+client_id+"> ");
                that_server.getClient(client_id).send(message);
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

    public void subscribe(String topic) throws IOException {
        if (subscribedTopicPartitions.containsKey(topic)) {
            return;
        }
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(this.groupId);

        Message request = new Message(MessageType.SUBSCRIBE_TOPIC, arguments);
        // send to coordinator and wait for partitions of this topic
        TcpClient consumerClient = new TcpClient(coordinator.getKey(), coordinator.getValue());
        consumerClient.setHandler(this.getClass(), this, request);
    }

    public void assignByRebalancePlan(Map<String, List<Pair<Integer, Broker>>> topicPartitions) {
        subscribedTopicPartitions = topicPartitions;
    }

    public List<ConsumerRecord> poll() {

        // multicast of each partition in subscribedPartitions;
        while (true) {
            // new Thread接收回传讯息
        }
    }


    Pair<String, Integer> pickBroker() throws IOException {
        Pair<String, Integer> broker = null;
        if (brokers.size() != 0) {
            defaultBrokerHost = brokers.get(0).getKey();
            defaultBrokerPort = brokers.get(0).getValue();
        } else {

        }
        return broker;
    }

    public void findCoordinator(String defaultBrokerHost, int defaultBrokerPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Message request = new Message(MessageType.CREATE_TOPIC.FIND_COORDINATOR, Collections.singletonList(this.groupId));
        // send request to defaultBroker with the groupId
        TcpClient sock = new TcpClient(defaultBrokerHost, defaultBrokerPort);
        sock.setHandler(this.getClass(), this, request);
        sock.run();
    }

    public void updateCoordinator(String host, Integer port) {
        coordinator = new Pair(host, port);
        System.out.println(("coordinator's host" + host));
        System.out.println(("coordinator's port" + host));
//        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK);
//        return response;
    }

    // to coordinator
    public void sendHeartBeat() {

    }
}

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
    HostRecord coordinator;
    TcpServerEventHandler serverHandler;
    TcpClientEventHandler consumerClientEventHandler;

    // default brokers and broker cache
    HostRecord defaultBroker;
    List<HostRecord> brokers;

    Map<String, Map<Integer, HostRecord>> subscribedTopicPartitions;


    // for leader of group
//    boolean isLeader;
    Map<String, Consumer> groupTopicAndConsumer;
    Map<String, List<Pair<Integer, Broker>>> groupTopicAndPartition;


    public void setToLeader() {
        serverSocket = new TcpServer(port);
//        setHandler();
    }

    public Consumer (String host, int port, String groupId, String defaultBrokerIp, int defaultBrokerPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.host = host;
        this.port = port;
        this.groupId = groupId;

        // ask default broker this group's coordinator (broker)
        this.defaultBroker = new HostRecord(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(this.defaultBroker);
        subscribedTopicPartitions = new HashMap<>();
    }


//    public void subscribe(String topic) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
//        if (subscribedTopicPartitions.containsKey(topic)) {
//            return;
//        }
//        List<Object> arguments = new ArrayList();
//        arguments.add(topic);
//        arguments.add(this.groupId);
//        arguments.add(new HostRecord(this.host, this.port));
//
//        Message request = new Message(MessageType.SUBSCRIBE_TOPIC, arguments);
//        // send to coordinator and wait for partitions of this topic
//        TcpClient consumerClient = new TcpClient(coordinator.getHost(), coordinator.getPort());
//        consumerClient.setHandler(this, request);
//        consumerClient.run();
//    }

    public void updateTopicPartition(Map<String, Map<Integer, HostRecord>> topicPartitions) {
        subscribedTopicPartitions = topicPartitions;
    }

    public Message rebalance(Map<String, List<HostRecord>> topic_consumers, Map<String, Map<Integer, HostRecord>> topic_partitions) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Map<HostRecord, Map<String, Map<Integer, HostRecord>>> rebalanceResult = new HashMap<>();
        for (Map.Entry<String, List<HostRecord>> eachTopic : topic_consumers.entrySet()) {
            List<HostRecord> consumerList = eachTopic.getValue();
            int indexConsumer = 0;
            int sizeCondumer = eachTopic.getValue().size();
            Iterator<Map.Entry<Integer, HostRecord>> it = topic_partitions.get(eachTopic.getKey()).entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, HostRecord> partition = it.next();// c1 : [topic1, [1,b1]
                Map<String, Map<Integer, HostRecord>> partitionOfConsumer = rebalanceResult.getOrDefault(consumerList.get(indexConsumer % sizeCondumer), new HashMap<>());   // c2 : [topic1, [2,b1]
                Map<Integer, HostRecord> partitionOfTopic = partitionOfConsumer.getOrDefault(eachTopic.getKey(), new HashMap<>());
                partitionOfTopic.put(partition.getKey(),partition.getValue());
                partitionOfConsumer.put(eachTopic.getKey(), partitionOfTopic);
                rebalanceResult.put(consumerList.get(indexConsumer % sizeCondumer), partitionOfConsumer);
                indexConsumer++;
            }
        }

        List<Object> arguments = new ArrayList<>();
        arguments.add(this.groupId);
        arguments.add(rebalanceResult);
        Message response = new Message(MessageType.REBALANCEPLAN, arguments);
        return response;
    }

    public List<ConsumerRecord> poll() {

        // multicast of each partition in subscribedPartitions;
        while (true) {
            // new Thread接收回传讯息
        }
    }


    void pickBroker() throws IOException {
        if (brokers.size() != 0) {
            defaultBroker = brokers.get(0);
        } else {

        }
    }

    public void receiveConsumerJoinGroupRegistrationAck(String ack) {
        System.out.println(ack);
    }


//    public void findCoordinator() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
//        Message request = new Message(MessageType.CREATE_TOPIC.FIND_COORDINATOR, Collections.singletonList(this.groupId));
//        // send request to defaultBroker with the groupId
//        TcpClient sock = new TcpClient(this.defaultBroker.getHost(), this.defaultBroker.getPort());
//        sock.setHandler(this, request);
//        sock.run();
//    }

    public void updateCoordinator(HostRecord coordinator) {
        this.coordinator = coordinator;
        System.out.println("My coordinator is " + coordinator.getPort());
    }

    // to coordinator
    public void sendHeartBeat() {

    }

    public static void main(String[] args) {
        Consumer xinzhuConsumer = null;
//        try {
//            xinzhuConsumer = new Consumer("localhost", 10001, "group1", "localhost", 9005);
//            xinzhuConsumer.findCoordinator();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (NoSuchMethodException e) {
//            e.printStackTrace();
//
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (InvocationTargetException e) {
//            e.printStackTrace();
//        }

    }
}

package com.scu.coen317;

import javafx.util.Pair;

import javax.jws.Oneway;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;


public class Consumer {
    String host;
    int port;
    String groupId;
    HostRecord thisHost;
    TcpServer serverSocket;
    HostRecord coordinator;

    // default brokers and broker cache
    HostRecord defaultBroker;
    List<HostRecord> brokers;

    Map<String, Map<Integer, HostRecord>> subscribedTopicPartitions;
    // 5 min;
    final int MAX_POLL_INTERVAL_MS = 3000;
    final int MAX_FETCH_SIZE = 10;

    public Consumer (String host, int port, String groupId, String defaultBrokerIp, int defaultBrokerPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        thisHost = new HostRecord(this.host = host, this.port = port);
        this.groupId = groupId;


        // ask default broker this group's coordinator (broker)
        this.defaultBroker = new HostRecord(defaultBrokerIp, defaultBrokerPort);
        brokers = new ArrayList();
        brokers.add(this.defaultBroker);
        subscribedTopicPartitions = new HashMap<>();
        serverSocket = new TcpServer(thisHost.getPort());
        serverSocket.setHandler(this);
        serverSocket.listen();
        /*findCoordinator();
        joinToGroup();
        */
    }

    public void joinToGroup() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        while (coordinator == null) {
            findCoordinator();
        }
        TcpClient client = new TcpClient(coordinator.getHost(), coordinator.getPort());
        List<Object> arguments = new ArrayList<>();
        arguments.add(this.groupId);
        arguments.add(this.thisHost);
        Message request = new Message(MessageType.JOIN_GROUP, arguments);
        client.setHandler(this, request);
        client.run();
    }


    public void initialLeader() {
        serverSocket = new TcpServer(thisHost.getPort());
        serverSocket.setHandler(this);
        serverSocket.listen();
    }

    public void updateTopicPartition(HashMap<String, Map<Integer, HostRecord>> topicPartitions) {
        subscribedTopicPartitions = topicPartitions;
        System.out.println("    --- New topicPartition updated in consumer");
        for (Map.Entry<String, Map<Integer, HostRecord>> en : topicPartitions.entrySet()) {
            System.out.println("        " + en.getKey() + " : " + en.getValue());
        }
    }

    public void subscribe(String topic) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        if (subscribedTopicPartitions.containsKey(topic)) {
            return;
        }
        subscribedTopicPartitions.put(topic, new HashMap<>());

        // send to coordinator and wait for partitions of this topic
        TcpClient consumerClient = new TcpClient(coordinator.getHost(), coordinator.getPort());
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(this.groupId);
        arguments.add(thisHost);
        Message request = new Message(MessageType.SUBSCRIBE_TOPIC, arguments);
        consumerClient.setHandler(this, request);
        consumerClient.run();
    }

    // Map<String, List<HostRecord>>, Map<String, Map<Integer, HostRecord>>
    // t1 : c1, t1 : []
    //
    public Message rebalance(HashMap<String, List<HostRecord>> topic_consumers, HashMap<String, Map<Integer, HostRecord>> topic_partitions) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        HashMap<HostRecord, Map<String, Map<Integer, HostRecord>>> rebalanceResult = new HashMap<>();
        for (Map.Entry<String, List<HostRecord>> eachTopic : topic_consumers.entrySet()) {
            List<HostRecord> consumerList = eachTopic.getValue();
            // t1 : c1, c2
            // t2 : c2
            // t1 : p1, p2

            // c1 : t1 ::
            // c2 : t1 ::
                  //t2 ::
            for (HostRecord consumer : consumerList) {
                Map<String, Map<Integer, HostRecord>> topicMap = rebalanceResult.getOrDefault(consumer, new HashMap<>());
                topicMap.put(eachTopic.getKey(), new HashMap<>());
                rebalanceResult.put(consumer, topicMap);
            }

            int indexConsumer = 0;
            int sizeConsumer = eachTopic.getValue().size();
            Iterator<Map.Entry<Integer, HostRecord>> it = topic_partitions.get(eachTopic.getKey()).entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, HostRecord> partition = it.next();// c1 : [topic1, [1,b1]
                Map<String, Map<Integer, HostRecord>> partitionOfConsumer = rebalanceResult.get(consumerList.get(indexConsumer % sizeConsumer));   // c2 : [topic1, [2,b1]
                Map<Integer, HostRecord> partitionOfTopic = partitionOfConsumer.get(eachTopic.getKey());
                partitionOfTopic.put(partition.getKey(),partition.getValue());
                partitionOfConsumer.put(eachTopic.getKey(), partitionOfTopic);
                rebalanceResult.put(consumerList.get(indexConsumer % sizeConsumer), partitionOfConsumer);
                indexConsumer++;
            }
        }

        List<Object> arguments = new ArrayList<>();
        arguments.add(this.groupId);
        arguments.add(rebalanceResult);
        Message response = new Message(MessageType.REBALANCEPLAN, arguments);
        return response;
    }

    public void poll() throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        // Map<String, Map<Integer, HostRecord>> subscribedTopicPartitions;
        // multicast of each partition in subscribedPartitions;

        List<String> messages = new ArrayList<>();
        while (true) {
            Thread.sleep(MAX_POLL_INTERVAL_MS);
            for (Map.Entry<String, Map<Integer, HostRecord>> eachTopic : subscribedTopicPartitions.entrySet()) {

                // System.out.println("Hello?");
                String topic = eachTopic.getKey();
                Map<Integer, HostRecord> partitions = eachTopic.getValue();
                for (Map.Entry<Integer, HostRecord> partition : partitions.entrySet()) {
//                    Thread.sleep(1000);

                    HostRecord broker = partition.getValue();
                    TcpClient client = new TcpClient(broker.getHost(), broker.getPort());
                    List<Object> arguments = new ArrayList<>();
                    arguments.add(groupId);
                    arguments.add(topic);
                    arguments.add(partition.getKey());
                    arguments.add(thisHost);
                    arguments.add(MAX_FETCH_SIZE);
                    Message request = new Message(MessageType.PULLMESSAGE, arguments);
                    // System.out.println("Ready to poll!!!");
                    client.setHandler(this, request);
                    client.run();
                }
            }
        }

    }

    public Message dealWithMessage(ArrayList<String> messages, String topic, HostRecord broker) {
        System.out.println("Enter consumer deal!!!");
        for (String message : messages) {
            System.out.println(message);
        }
        List<Object> arguments = new ArrayList<>();
        arguments.add("Message in topic of " + topic
                + " received successful from " + broker);

        Message responseAck = new Message(MessageType.ACK, arguments, true);
        return responseAck;
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


    public void findCoordinator() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        List<Object> arguments = new ArrayList<>();
        arguments.add(groupId);
        arguments.add(thisHost);
        Message request = new Message(MessageType.CREATE_TOPIC.FIND_COORDINATOR, arguments);
        // send request to defaultBroker with the groupId
        TcpClient sock = new TcpClient(this.defaultBroker.getHost(), this.defaultBroker.getPort());
        sock.setHandler(this, request);
        sock.run();
    }

    public Message updateCoordinator(HostRecord coordinator) {
        this.coordinator = coordinator;
        Message ack = new Message(MessageType.ACK, Collections.singletonList("Coordinator updated successful"), true);
        return ack;
    }

    // to coordinator
    public void sendHeartBeat() {

    }

    public Message test1() {
        Message response = new Message(MessageType.TEST1);
        return response;
    }

    public Message test2() {
        System.out.println("received message from broker1");
        return new Message(MessageType.ACK, Collections.singletonList("consumer received message from broker1"), true);
    }

}

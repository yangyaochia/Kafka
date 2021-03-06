package com.scu.coen317;

import javafx.util.Pair;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;

public class Broker {
    HostRecord thisHost;
    TcpServer listenSocket;
    HostRecord defaultZookeeper;

    // The whole information about partition leaders for each topic
    Map<String, Map<Integer, List<String>>> topicMessage;
    DataCache<String, Map<Integer, HostRecord>> topicsPartitionLeader;

    // Map<topic,Map<partition,List<replicationHolders>>
    Map<String, Map<Integer, Set<HostRecord>>> topicPartitionReplicationBrokers;
    Map<String, HostRecord> topics_coordinator;

   /* 
    * as coordinator for consumer group
    * track the info of each consumer in the group 
    */ 
    Map<String, Map<String, List<HostRecord>>> topic_consumer;
    Map<String, List<String>> group_topic;
    DataCache<String, Map<Integer, HostRecord>> topicsPartitionLeaderCache;

    // track each group's consumer member and theirs subscribed topics, and respective partition leaders
    Map<String, Map<HostRecord, Map<String, Map<Integer, HostRecord>>>> balanceMap;
    // each group's leader
    Map<String, HostRecord> consumerLeader;

    // balance Map for each group
    Map<Consumer, Map<String, Integer>> consumerOffset;

    // Map<topic,Map<partition,Map<groupId,offset>>>
    Map<String, Map<Integer, Map<String,Integer>>> consumerGroupOffset;
    final int HEART_BEAT_INTERVAL = 400;

    public Broker(String host, int port, String zookeeperHost, int zookeeperPort) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.thisHost = new HostRecord(host, port);
        this.defaultZookeeper = new HostRecord(zookeeperHost, zookeeperPort);
        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);
        topicMessage = new HashMap<>();
        topicsPartitionLeader = new DataCache<>();
        topicsPartitionLeader.setTimeout(10);
        topicPartitionReplicationBrokers = new HashMap<>();
        topic_consumer = new HashMap<>();
        topics_coordinator = new HashMap();
        consumerLeader = new HashMap();
        consumerOffset = new HashMap();
        consumerGroupOffset = new HashMap<>();
        balanceMap = new HashMap<>();
        topic_consumer = new HashMap<>();
        group_topic = new HashMap<>();
        topicsPartitionLeaderCache = new DataCache<>();
        topicsPartitionLeaderCache.setTimeout(1);
    }

    public Message getTopic(Topic topic, HostRecord producer) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {
        String topicName = topic.getName();
        Message response;

        // This broker does now know the topic, then ask the zookeeper
        topicsPartitionLeader.remove(topicName);
        if ( !topicsPartitionLeader.containsKey(topicName) ) {
            List<Object> argument = new ArrayList<>();
            argument.add(topic);
            argument.add(thisHost);
            Message request = new Message(MessageType.GET_TOPIC, argument);
            TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
            sock.setHandler( this, request);
            sock.run();
        }

        // This broker already stored the topic info
        synchronized (this) {
            while (!topicsPartitionLeader.containsKey(topicName)) {
                wait();
            }
        }
        
        List<Object> argument = new ArrayList<>();
        argument.add(topicName);
        argument.add(topicsPartitionLeader.get(topicName) );
        response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_PRODUCER, argument);
        TcpClient sock = new TcpClient(producer.getHost(), producer.getPort());
        sock.setHandler(this, response);
        sock.run();
        return new Message(MessageType.ACK);
    }
    
    public void topicAssignmentToProducer(Topic topic, HashMap<Integer,HostRecord> partitionLeaders) {
        System.out.println("Broker > Partition leader assigning ");
        for (Integer name: partitionLeaders.keySet()){
            String key =name.toString();
            partitionLeaders.get(name).toString();
            System.out.println("Broker > Partition leader assigned. ");
        }
        synchronized (this) {
            topicsPartitionLeader.put(topic.getName(), partitionLeaders);
            notify();
        }
        return;
    }

    public void setTopicPartitionLeader(String topic, Integer partition, HostRecord leader, HashSet<HostRecord> replicationHolders) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, InterruptedException {
        if (topicMessage.get(topic) == null) {
            // Case 1 : This broker has not known this topic partition ever
            Map<Integer, List<String>> partitionMap = new HashMap<>();
            partitionMap.put(partition, new ArrayList());
            topicMessage.put(topic, partitionMap);
        } else if ( topicMessage.get(topic).get(partition) == null ) {
            // Case 2: This broker has not known this partition ever
            topicMessage.get(topic).put(partition, new ArrayList<>());
        }

        if ( topicPartitionReplicationBrokers.get(topic) == null ) {
            Map<Integer, Set<HostRecord>> partitionHolderMap = new HashMap<>();
            partitionHolderMap.put(partition, replicationHolders);
            topicPartitionReplicationBrokers.put(topic, partitionHolderMap);
        } else if ( topicPartitionReplicationBrokers.get(topic).get(partition) == null ) {
            topicPartitionReplicationBrokers.get(topic).put(partition, replicationHolders);
        }

        if ( consumerGroupOffset.get(topic) == null ) {
            Map<Integer, Map<String,Integer>> partitionMap = new HashMap<>();
            partitionMap.put(partition, new HashMap<>());
            consumerGroupOffset.put(topic, partitionMap);
        } else if ( consumerGroupOffset.get(topic).get(partition) == null ) {
            consumerGroupOffset.get(topic).put(partition,new HashMap<>());
        }

        System.out.println("Broker > Leader : " + leader.getPort());
        for ( HostRecord h: replicationHolders ) {
            System.out.println("Broker > Replication : " + h.getPort());
        }

        if ( leader.equals(thisHost) ) {
            List<Object> argument = new ArrayList<>();
            argument.add(topic);
            argument.add(partition);
            argument.add(leader);
            argument.add(replicationHolders);
            Message request = new Message(MessageType.SET_TOPIC_PARTITION_LEADER, argument);
            informOtherBrokers(request, replicationHolders);
        }
    }
    
    public void informOtherBrokers(Message request, HashSet<HostRecord> otherBrokers) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        for ( HostRecord h : otherBrokers ) {
            System.out.println("Broker > This leader host " + thisHost.getPort());
            System.out.println("Broker > Send to " + h.getPort());
            TcpClient sock = new TcpClient(h.getHost(), h.getPort());
            sock.setHandler( this, request);
            sock.run();
        }
        return;
    }

    public Message publishMessage(String topic, Integer partition, String message, HostRecord producer) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {
        List<String> msgs = topicMessage.get(topic).get(partition);
        synchronized (this) {
            msgs.add(message);
        }

        Set<HostRecord> replicationHolders = topicPartitionReplicationBrokers.get(topic).get(partition);
        if ( !replicationHolders.contains(thisHost)) {
            List<Object> argument = new ArrayList<>();
            argument.add(topic);
            argument.add(partition);
            argument.add(message);
            argument.add(thisHost);
            Message request = new Message(MessageType.PUBLISH_MESSAGE, argument);
            informOtherBrokers(request, (HashSet<HostRecord>) replicationHolders);
        }

        List<Object> arguments = new ArrayList<>();
        arguments.add(message);
        arguments.add("");
        Message response = new Message(MessageType.PUBLISH_MESSAGE_ACK, arguments);
        TcpClient sock = new TcpClient(producer.getHost(),producer.getPort());
        sock.setHandler(this, response);
        sock.run();
        System.out.println("Broker > Published message success from Producer" + producer.getPort() + " *** " );
        return new Message(MessageType.ACK);
    }
    
    public void publishMessageAck(String message, String ackMessage) {
        System.out.println("Broker > " + message + " " + ackMessage + " *** ");
    }

    public Message giveMessage(String groudID, String topic, Integer partition, HostRecord consumer, Integer maxFetchSize) throws InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException, IOException {
        Message response;
        HashMap<String,Integer> consumerGroup = (HashMap<String, Integer>) consumerGroupOffset.get(topic).get(partition);

        if ( !consumerGroup.containsKey(groudID) ) {
            consumerGroup.put(groudID,0);
        }
        
        List<String> sendingMessages;
        synchronized (this) {
            List<String> topicPartitionMessage = topicMessage.get(topic).get(partition);
            System.out.println("Broker > Current message size : " + topicPartitionMessage.size());
            int offset = consumerGroup.get(groudID);
            System.out.println("Broker > offset : " + offset);
            if ( offset == topicPartitionMessage.size() ){
                return new Message(MessageType.ACK);
            }
            int maxOffset = Integer.min(offset + maxFetchSize, topicPartitionMessage.size());
            consumerGroup.put(groudID,maxOffset);
            System.out.println("Broker > offset : " + offset + " maxOffset : " + maxOffset);
            sendingMessages = new ArrayList<>(topicPartitionMessage.subList(offset, maxOffset));
        }

        System.out.println("Broker > sendingMessages.size() : " + sendingMessages.size());
        List<Object> argument = new ArrayList<>();
        argument.add(sendingMessages);
        argument.add(topic);
        argument.add(thisHost);
        response = new Message(MessageType.SEND_MESSAGE_TO_CONSUMER, argument);
        TcpClient sock = new TcpClient(consumer.getHost(),consumer.getPort());
        sock.setHandler(this,response);
        sock.run();
        return new Message(MessageType.ACK);
    }

    public void receiveNewBrokerRegistrationAck(String message) {
        return;
    }

    public void registerToZookeeper() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        TcpClient client = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
        List<Object> arguments = new ArrayList<>();
        arguments.add(this.thisHost);
        Message request = new Message(MessageType.NEW_BROKER_REGISTER, arguments);
        client.setHandler(this, request);
        client.run();
    }

    public Message getCoordinator(String groupId, HostRecord consumer) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        while (!topics_coordinator.containsKey(groupId)) {
            TcpClient client = new TcpClient(defaultZookeeper.host, defaultZookeeper.port);
            List<Object> arguments = new ArrayList<>();
            arguments.add(groupId);
            arguments.add(this.thisHost);
            Message request = new Message(MessageType.GET_COORDINATOR, arguments);
            client.setHandler(this, request);
            client.run();
        }
        
        TcpClient tcpClient = new TcpClient(consumer.getHost(), consumer.getPort());
        HostRecord coordinator = topics_coordinator.get(groupId);
        List<Object> arguments = new ArrayList();
        arguments.add(coordinator);
        Message request = new Message(MessageType.UPDATE_COORDINATOR, arguments);
        tcpClient.setHandler(this,request);
        tcpClient.run();
        Message ack = new Message(MessageType.ACK, Collections.singletonList(" *** Get coordinator successful *** "), true);
        return ack;
    }

    public Message storeInfoAndGetTopicAndRebalance(String topic, String groupId, HostRecord consumer) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {

        if (storeInfoAndGetTopic(topic, groupId, consumer)) {
            rebalance(groupId, consumer);
            return new Message(MessageType.ACK, Collections.singletonList(" *** Subscribe successful *** "), true);
        }
        return new Message(MessageType.ACK, Collections.singletonList(" *** Subscribed failed, no such topic found in the system"), true);
    }

    public void updateCoordinator(String groupId, HostRecord coordinator) {
        System.out.println("add coordinator " + coordinator.getHost() + " " + coordinator.getPort()
                + " to " + groupId);
        topics_coordinator.put(groupId, coordinator);
        System.out.println("Broker > Added coordinator " + coordinator.getHost() + " " + coordinator.getPort()
                + " to " + groupId);
    }

    public boolean storeInfoAndGetTopic(String topic, String groupId, HostRecord consumer) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        Map<String, List<HostRecord>> topic_subscribedConsumer = topic_consumer.getOrDefault(groupId, new HashMap<>());
        if (topic_subscribedConsumer.containsKey(topic)) {
            if (topic_subscribedConsumer.get(topic).contains(consumer)) {
                return false;
            }
            topic_subscribedConsumer.get(topic).add(consumer);
            topic_consumer.put(groupId, topic_subscribedConsumer);
        } else {
            topic_subscribedConsumer.put(topic, new ArrayList(Arrays.asList(consumer)));
            topic_consumer.put(groupId, topic_subscribedConsumer);
            List<String> topics = group_topic.getOrDefault(groupId, new ArrayList<>());
            topics.add(topic);
            group_topic.put(groupId, topics);
        }

        while (!topicsPartitionLeaderCache.containsKey(topic)) {
            getTopic(topic);
        }
        if (topicsPartitionLeaderCache.get(topic).size() == 0) {
            deleteTopic(groupId, topic);
            return false;
        }
        return true;
    }

    public void deleteTopic(String groupId, String topic) {
        topic_consumer.get(groupId).remove(topic);
        topicsPartitionLeaderCache.remove(topic);
        group_topic.remove(topic);
    }

    public void rebalance(String groupId, HostRecord consumer) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        // assign group leader to rebalance 
        balanceMap.remove(groupId);
        Map<String, Map<Integer, HostRecord>> topic_partitions = new HashMap<>();
        for (String topic : group_topic.get(groupId)) {
            topic_partitions.put(topic, topicsPartitionLeaderCache.get(topic));
        }
        if (topic_partitions.size() == 0) {
            balanceMap.put(groupId, new HashMap<>());
        } else {
            while (!balanceMap.containsKey(groupId)) {
                HostRecord leader = consumerLeader.get(groupId);

                List<Object> arguments = new ArrayList<>();
                arguments.add(topic_consumer.get(groupId));
                // Map<String, List<HostRecord>>
                arguments.add(topic_partitions);
                // Map<String, Map<Integer, HostRecord>>

                Message request = new Message(MessageType.REBALANCE, arguments);
                TcpClient socket = new TcpClient(leader.getHost(), leader.getPort());
                socket.setHandler(this, request);
                socket.run();
            }
            assignNewBalance(groupId);
        }
    }

    public void assignNewBalance(String groupId) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        // multicast
        // Map<String, Map<HostRecord, Map<String, Map<Integer, HostRecord>>>> balanceMap;
        Map<HostRecord, Map<String, Map<Integer, HostRecord>>> map = balanceMap.get(groupId);
        for (HostRecord consumerInMap : map.keySet()) {
            assignPartitionToConsumer(consumerInMap, map.get(consumerInMap));
        }
    }

    public void assignPartitionToConsumer(HostRecord consumer, Map<String, Map<Integer, HostRecord>> topic_partitions) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        TcpClient client = new TcpClient(consumer.getHost(), consumer.getPort());
        List<Object> arguments = new ArrayList<>();
        arguments.add(topic_partitions);
        Message request = new Message(MessageType.REBALANCE_RESULT, arguments);
        client.setHandler(this, request);
        client.run();
    }

    public void updateConsumerLeader(String groupId, HostRecord consumer) {
        consumerLeader.put(groupId, consumer);
    }

    public void assignLeader(HostRecord consumer) throws IOException {
        TcpClient client = new TcpClient(consumer.getHost(), consumer.getPort());
        Message request = new Message(MessageType.INITIAL_LEADER);
        client.setHandler(this, request);
    }
    
    // find specific topic's partition leaders
    public void getTopic(String topicName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException {
        // This broker does not know the topic, then ask the zookeeper
        if (!topicsPartitionLeaderCache.containsKey(topicName)) {
            List<Object> arguments = new ArrayList<>();
            arguments.add(topicName);
            arguments.add(thisHost);
            Message request = new Message(MessageType.GET_TOPIC_FOR_COORDINATOR, arguments);

            TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
            sock.setHandler(this, request);
            sock.run();
        }
    }

    public void updateTopicsPartitionLeaderCache(String topic, HashMap<Integer, HostRecord> topicPartitionLeaders) {
        topicsPartitionLeaderCache.put(topic, topicPartitionLeaders);
        System.out.println("Broker > Topic partitions update.");
    }

    public Message addConsumerToGroup(String groupId, HostRecord consumer) throws IOException {
        if (!topic_consumer.containsKey(groupId)) {
            updateConsumerLeader(groupId, consumer);
        }

        List<Object> arguments = new ArrayList<>();
        arguments.add("Broker > *** Consumer Registered Successful *** ");
        Message response = new Message(MessageType.ACK, arguments, true);
        System.out.println("Broker > Consumer added into the group successful");
        return response;
    }

    public void updateBalanceMap(String groupId, HashMap<HostRecord, Map<String, Map<Integer, HostRecord>>> newBalance) {
        balanceMap.put(groupId, newBalance);
    }

    public void replaceTopicPartitionLeader(HashMap<String, Map<Integer, Map<HostRecord, HostRecord>>> newInfo) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InterruptedException, IOException {
        // Map<String, Map<HostRecord, Map<String, Map<Integer, HostRecord>>>> balanceMap;
        System.out.println("Broker > Topic partition leaders start to replace...");
        Set<Pair<String, HostRecord>> changeSet = new HashSet<>();

        for (Map.Entry<String, Map<HostRecord, Map<String, Map<Integer, HostRecord>>>> eachGroup : balanceMap.entrySet()) {
            String groupId = eachGroup.getKey();
            for (Map.Entry<HostRecord, Map<String, Map<Integer, HostRecord>>> eachConsumer : balanceMap.get(groupId).entrySet()) {
                HostRecord consumer = eachConsumer.getKey();
                for (Map.Entry<String, Map<Integer, HostRecord>> eachTopic : balanceMap.get(groupId).get(consumer).entrySet()) {
                    String curTopic = eachTopic.getKey();
                    if (newInfo.containsKey(curTopic)) {
                        Map<Integer, Map<HostRecord, HostRecord>> partitionBroker = newInfo.get(curTopic);
                        for (Map.Entry<Integer, HostRecord> eachPartition : balanceMap.get(groupId).get(consumer).get(curTopic).entrySet()) {
                            Integer partition = eachPartition.getKey();
                            if (partitionBroker.containsKey(partition)) {
                                HostRecord broker = eachPartition.getValue();
                                if (eachPartition.getValue().equals(broker)) {
                                    changeSet.add(new Pair(groupId,consumer));
                                    System.out.println("Broker > add " + consumer + " to set");
                                    HostRecord newBroker = partitionBroker.get(partition).get(broker);
                                    balanceMap.get(groupId).get(consumer).get(curTopic).put(partition, newBroker);
                                }
                            }

                        }

                        for (Pair<String, HostRecord> consumerNeedToReassign : changeSet) {
                            assignPartitionToConsumer(consumerNeedToReassign.getValue(), balanceMap.get(groupId).get(consumerNeedToReassign.getValue()));
                            System.out.println("Broker > Topic partition leaders updated.");
                        }
                    }
                }
            }
        }
    }

    public void updateTopicsPartitionLeader(String topic, Map<Integer, HostRecord> topicPartitionLeaders) {
        topicsPartitionLeader.put(topic, topicPartitionLeaders);
    }

    public void listen() throws
            IOException, ClassNotFoundException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        listenSocket.listen();
        System.out.println("Broker > The server is listening at " + thisHost.getHost() + " " + thisHost.getPort());
    }
    
    public void sendHeartBeat() throws InterruptedException {
        while (true) {
            // Send hearbeat
            Thread.sleep(HEART_BEAT_INTERVAL);
            try {
                List<Object> argument = new ArrayList<>();
                argument.add(thisHost);
                Message heartbeat = new Message(MessageType.SEND_HEARTBEAT, argument);
                TcpClient sock = new TcpClient(defaultZookeeper.getHost(), defaultZookeeper.getPort());
                sock.setHandler( this, heartbeat);
                sock.run();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}


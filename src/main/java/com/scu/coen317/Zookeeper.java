package com.scu.coen317;
import javafx.util.Pair;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.sql.Timestamp;
import java.util.*;
import static java.lang.Math.min;

public class Zookeeper {
    String host;
    int port;
    TcpServer listenSocket;
    Queue<HostRecord> topicBrokerQueue;
    Queue<HostRecord> coordinatorBrokerQueue;
    HashMap<String, HashMap<Integer, HostRecord>> topicAssignmentHash;
    HashMap<String, HostRecord> coordinatorAssignmentHash;
    Map<String, Map<Integer,Map<HostRecord, Set<HostRecord>>>> replicationHash;
    Set<HostRecord> brokerList;
    Set<HostRecord> tempBrokerList;
    Map<HostRecord, Map<String, Integer>> brokerToTopicPartitionHash;

    final int MONITOR_CLUSTER_INTERVAL = 4000;

    public Zookeeper(String host, int port) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.host = host;
        this.port = port;
        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);
        topicAssignmentHash = new HashMap<>() ;
        replicationHash = new HashMap<>();
        brokerList = new HashSet<>();
        topicBrokerQueue = new LinkedList<>();
        coordinatorBrokerQueue = new LinkedList<>();
        coordinatorAssignmentHash = new HashMap<>();
        brokerToTopicPartitionHash = new HashMap<>();

        this.tempBrokerList = new HashSet<>();
        this.brokerToTopicPartitionHash = new HashMap<>();
    }

    public void displayAllQueue() {
        System.out.println("topicBrokerQueue: ");
        displayOneQueue(topicBrokerQueue);
        System.out.println("coordinatorBrokerQueue: ");
        displayOneQueue(coordinatorBrokerQueue);
    }

    public void displayOneQueue(Queue<HostRecord> oneQueue ) {
        for (HostRecord item : oneQueue) {
            System.out.println(item.toString());
        }
    }

    public void displayMapFailNewBroker(Map<String, Map<Integer, Map<HostRecord, HostRecord>>> oneHash) {
        for (String topic : oneHash.keySet()) {
            for (Integer partition : oneHash.get(topic).keySet()) {
                for (HostRecord leader : oneHash.get(topic).get(partition).keySet()) {
                    System.out.println("Topic : " + topic + " partition: " + partition);
                    System.out.println("Fail Leader: "+ leader.toString());
                    System.out.println("New Leader: "+ oneHash.get(topic).get(partition).get(leader));
                }
            }
        }
    }

    public void removeFailLeader(Set<HostRecord> failLeaders) {
        for(HostRecord leader : failLeaders) {
            brokerToTopicPartitionHash.remove(leader);
            topicBrokerQueue.remove(leader);
            coordinatorBrokerQueue.remove(leader);
            brokerList.remove(leader);
        }
    }

    public Map<String, Map<Integer, Map<HostRecord, HostRecord>>> reAssignLeader(Set<HostRecord> failLeaders) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InterruptedException, IOException {
        removeFailLeader(failLeaders);
        Map<String, Map<Integer, Map<HostRecord, HostRecord>>> failHash = new HashMap<>();
        for (String topic : replicationHash.keySet()) {
            for (Integer partition: replicationHash.get(topic).keySet()) {
                for (HostRecord leader: replicationHash.get(topic).get(partition).keySet()) {
                    HashSet<HostRecord> oldFollowers = (HashSet<HostRecord>) replicationHash.get(topic).get(partition).get(leader);
                    
                    Iterator<HostRecord> iterator = oldFollowers.iterator();
                    while (iterator.hasNext()) {
                        HostRecord element = iterator.next();
                        if (failLeaders.contains(element)) {
                            iterator.remove();
                        }
                    }
                    
                    if (failLeaders.contains(leader)) {
                        if (!replicationHash.get(topic).get(partition).get(leader).isEmpty()) {
                            HostRecord newLeader = oldFollowers.iterator().next();
                            Iterator<HostRecord> iterator2 = oldFollowers.iterator();
                            while (iterator2.hasNext()) {
                                HostRecord element = iterator2.next();
                                if (element.equals(newLeader)) {
                                    iterator2.remove();
                                }
                            }

                            HashSet<HostRecord> newFollowers = oldFollowers;
                            replicationHash.get(topic).get(partition).put(newLeader, newFollowers);
                            topicAssignmentHash.get(topic).put(partition, newLeader);

                            if(!brokerToTopicPartitionHash.containsKey(newLeader)) {
                                brokerToTopicPartitionHash.put(newLeader, new HashMap<>());
                            }
                            
                            brokerToTopicPartitionHash.get(newLeader).put(topic, partition);
                            if(!failHash.containsKey(topic)) {
                                failHash.put(topic, new HashMap<>());
                            }
                            
                            if(!failHash.get(topic).containsKey(partition)) {
                                failHash.get(topic).put(partition, new HashMap<>());
                            }
                            
                            failHash.get(topic).get(partition).put(leader, newLeader);
                            sendSetTopicPartitionLeader(topic, partition, newLeader, newFollowers);
                        } else {
                            System.out.println("failLeader: "+ leader.toString());
                            System.out.println("Topic: "+topic+" Partition: "+partition);
                            System.out.println("Has no replicas already");
                        }
                    }
                }
            }
        }
        return failHash;
    }

    public void sendLastestAssignmentToCoordinator(Map<String, Map<Integer, Map<HostRecord, HostRecord>>> latestAssignment) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException
    {
        List<Object> arguments = new ArrayList();
        arguments.add(latestAssignment);
        for(HostRecord coordinator: coordinatorAssignmentHash.values())
        {
            Message response = new Message(MessageType.REPLACE_BROKER, arguments);
            TcpClient sock = new TcpClient(coordinator.getHost(), coordinator.getPort());
            sock.setHandler( this, response );
            sock.run();
        }
    }

    public void monitorCluster() throws InterruptedException, NoSuchMethodException, IOException, IllegalAccessException, InvocationTargetException {
        while (true) {
            tempBrokerList = new HashSet<>(brokerList);
            Thread.sleep(MONITOR_CLUSTER_INTERVAL);

            if (!tempBrokerList.isEmpty()) {
                for (HostRecord item: tempBrokerList) {
                    System.out.println("Zookeeper > Broken Broker's port : " + item);
                }

                Map<String, Map<Integer, Map<HostRecord, HostRecord>>> latestAssignment = reAssignLeader(tempBrokerList);
                sendLastestAssignmentToCoordinator(latestAssignment);
                System.out.println("Zookeeper > In the cluster now we have " + brokerList.size() + " brokers.");
            }
        }
    }

    public void updateCluster(HostRecord healthyHeartBeat) {
        synchronized (this) {
            tempBrokerList.remove(healthyHeartBeat);
        }
    }

    public Message coordinatorAssignment(String groupID, HostRecord sender) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        System.out.println("Zookeeper > Coordinator assigning...");

        if (!coordinatorAssignmentHash.containsKey(groupID)) {
            assignCoordinator(groupID);
        }
        HostRecord temp = coordinatorAssignmentHash.get(groupID);
        System.out.println("Zookeeper > Coordinator assigned.");
        List<Object> arguments = new ArrayList();
        arguments.add(groupID);
        arguments.add(temp);

        Message response = new Message(MessageType.COORDINATOR_ASSIGNMENT, arguments);
        TcpClient sock = new TcpClient(sender.getHost(), sender.getPort());
        sock.setHandler( this, response );
        sock.run();
        return response;
    }

    public void assignCoordinator(String groupID) {
        synchronized (this) {
            HostRecord tempBroker = coordinatorBrokerQueue.poll();
            coordinatorAssignmentHash.put(groupID, tempBroker);
            System.out.println("Coordinator for Group " + groupID + " " + tempBroker.toString());
            coordinatorBrokerQueue.add(tempBroker);
        }
    }

    public void createTopicAssignment(Topic topic) {
        System.out.println("Zookeeper > Topic assignment creating...");
        for (Integer i = 0; i < topic.partition; i++) {
            HostRecord tempBroker = topicBrokerQueue.poll();
            
            if (!topicAssignmentHash.containsKey(topic.getName())) {
                HashMap<Integer, HostRecord> h = new HashMap<>();
                h.put(i, tempBroker);
                topicAssignmentHash.put(topic.getName(), h);
            } else {
                topicAssignmentHash.get(topic.getName()).put(i, tempBroker);
            }
            
            if (!brokerToTopicPartitionHash.containsKey(tempBroker)) {
                HashMap<String, Integer> t = new HashMap<>();
                t.put(topic.getName(), i);
                brokerToTopicPartitionHash.put(tempBroker, t);
            } else {
                brokerToTopicPartitionHash.get(tempBroker).put(topic.getName(), i);
            }
            topicBrokerQueue.add(tempBroker);
        }
        System.out.println("Zookeeper > Topic assignment created.");
    }

    public Message topicPartitions(String topic, HostRecord coordinator) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        List<Object> arguments = new ArrayList();
        HashMap<Integer, HostRecord> result = new HashMap<>();

        if (topicAssignmentHash.containsKey(topic)) {
            result = topicAssignmentHash.get(topic);
        }
        
        TcpClient returnNewPartition = new TcpClient(coordinator.getHost(), coordinator.getPort());
        arguments.add(topic);
        arguments.add(result);
        Message request = new Message(MessageType.RETURN_TOPIC_FOR_COORDINATOR, arguments);
        returnNewPartition.setHandler(this, request);
        returnNewPartition.run();
        return new Message(MessageType.ACK, Collections.singletonList(" *** partition leaders send successful *** "), true);
    }

    public Message topicAssignment(Topic topic, HostRecord sender) throws InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {

        if(!topicAssignmentHash.containsKey(topic.getName())) {
            createTopicAssignment(topic);
        }
        
        displayTopicAssignment(topic.getName());
        leaderReplicasAssignment(topic);
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(topicAssignmentHash.get(topic.getName()));
        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
        TcpClient sock = new TcpClient(sender.getHost(), sender.getPort());
        sock.setHandler( this, response );
        sock.run();
        return response;
    }

    public void leaderReplicasAssignment(Topic topic) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        System.out.println("Zookeeper > ReplicationAssignment...");
        HashMap<Integer, HostRecord> partitions = topicAssignmentHash.get(topic.getName());
        for (Integer partition: partitions.keySet()) {
            HashSet<HostRecord> temp = new HashSet();
            int repCount = min(topicBrokerQueue.size(),topic.replication);
            
            for (Integer i=0; i<repCount-1;i++) {
                HostRecord tempRep = topicBrokerQueue.poll();
                if (!tempRep.equals(partitions.get(partition))) {
                    temp.add(tempRep);
                } else {
                    i--;
                }
                topicBrokerQueue.add(tempRep);
            }
            
            if(!replicationHash.containsKey(topic.getName())) {
                replicationHash.put(topic.getName(), new HashMap<>());
            }
            
            if(!replicationHash.get(topic.getName()).containsKey(partition)) {
                replicationHash.get(topic.getName()).put(partition, new HashMap<>());
            }
            
            replicationHash.get(topic.getName()).get(partition).put(partitions.get(partition), temp);
            for (HostRecord follower : temp ) {
                if (!brokerToTopicPartitionHash.containsKey(follower)) { 
                    brokerToTopicPartitionHash.put(follower, new HashMap<>());
                }
                brokerToTopicPartitionHash.get(follower).put(topic.getName(), partition);
            }

            HashSet<HostRecord> followers = (HashSet<HostRecord>) replicationHash.get(topic.getName()).get(partition).get(partitions.get(partition));
            sendSetTopicPartitionLeader(topic.getName(), partition, partitions.get(partition), followers);

            String key =partition.toString();
            partitions.get(partition).toString();
            System.out.println("Zookeeper > Partition " +key.toString());
            System.out.println("          > Leader :  " + partitions.get(partition).getHost() + " " + partitions.get(partition).getPort());
            System.out.println("          > Followers : ");
            for (HostRecord oneFollower : replicationHash.get(topic.getName()).get(partition).get(partitions.get(partition))) {
                System.out.println("                      " + oneFollower.getHost() + " "+ oneFollower.getPort());
            }
        }
    }

    public void sendSetTopicPartitionLeader(String topic, Integer partition, HostRecord leader, HashSet<HostRecord> followers ) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(partition);
        arguments.add(leader);
        arguments.add(replicationHash.get(topic).get(partition).get(leader));
        Message assignment = new Message(MessageType.SET_TOPIC_PARTITION_LEADER, arguments);
        TcpClient sock = new TcpClient(leader.getHost(), leader.getPort());
        sock.setHandler( this, assignment );
        sock.run();
    }

    public void displayRelicationsHash(String topic) {
        HashMap<Integer, HostRecord> partitions = topicAssignmentHash.get(topic);
        for (Integer name: partitions.keySet()){
            String key = name.toString();
            partitions.get(name).toString();
        }
    }

    public void displayTopicAssignment(String topic) {
        HashMap<Integer, HostRecord> partitions = topicAssignmentHash.get(topic);
        for (Integer name: partitions.keySet()) {
            String key =name.toString();
            partitions.get(name).toString();
        }
    }

    public Message topicAssignmentToBroker() {
        List<Object> arguments = new ArrayList<>();
        String temp = " *** Topic assignment sucesssful ***  ";
        arguments.add(temp);
        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
        response.setIsAck(true);
        return response;
    }

    public Message newBrokerRegister(HostRecord oneBroker) {
        System.out.println("Zookeeper > Start to register for broker...");   //如果在這掛掉怎辦
        List<Object> arguments = new ArrayList();
        if (!containsBroker(oneBroker)) {
            brokerList.add(oneBroker);
            coordinatorBrokerQueue.add(oneBroker);
            topicBrokerQueue.add(oneBroker);

            System.out.println("Zookeeper > Broker at port " + oneBroker.getPort() + " register completed");  
            String temp = " *** " + oneBroker.toString() + " register to zookeeper Success *** ";
            arguments.add(temp);
            displayBrokerList();
            Message response = new Message(MessageType.ACK, arguments, true);
            System.out.println("Zookeeper > In the cluster now we have " + brokerList.size() + " broker(s).");
            return response;
        }
        
        String temp = " *** Already Registered *** ";
        arguments.add(temp);
        Message response = new Message(MessageType.ACK, arguments, true);
        return response;
    }

    public void displayBrokerList(){
        for (HostRecord aBroker: brokerList) {
        }
    }

    public boolean containsBroker(HostRecord oneBroker) {
        for (HostRecord aBroker : brokerList) {
            if (aBroker.getHost().equalsIgnoreCase(oneBroker.getHost()) && aBroker.getPort().equals(oneBroker.getPort())) {
                return true;
            }
        }
        return false;
    }

    public void listen() throws IOException, ClassNotFoundException, InterruptedException {
        listenSocket.listen();
        System.out.println("Zookeeper > The server is listening at " + this.host + " " + this.port);
    }
}

package com.scu.coen317;

import javafx.util.Pair;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.sql.Timestamp;
import java.util.*;

import static java.lang.Math.min;


//class HostWithTimeComparator implements Comparator<HostWithTime> {
//    @Override
//    public int compare(HostWithTime comm1, HostWithTime comm2) {
//        long t1 = comm1.getTimeStamp().getTime();
//        long t2 = comm2.getTimeStamp().getTime();
//        if(t2 > t1)
//            return -1;
//        else if(t1 > t2)
//            return 1;
//        else
//            return 0;
//    }
//}

public class Zookeeper {
    String host;
    int port;
    TcpServer listenSocket;
//    Map<String, List<String>> topicMessage;
    Queue<HostRecord> topicBrokerQueue;
    Queue<HostRecord> coordinatorBrokerQueue;
    HashMap<String, HashMap<Integer, HostRecord>> topicAssignmentHash;
    HashMap<String, HostRecord> coordinatorAssignmentHash;
//    Map<HostRecord, Map<String, Map<Integer, Set<HostRecord>>>> replicationsHash;
//    Map<HostRecord, Map<Pair<String, Integer>, Set<HostRecord>>> replicationHash;
    Map<String, Map<Integer,Map<HostRecord, Set<HostRecord>>>> replicationHash;
    Map< HostRecord,Map<String,Integer> > brokerToTopicPartitionHash;

    Queue<HostRecord> replicationBrokerQueue;
    Integer updateClusterInterval;
    Set<HostRecord> brokerList, tempBrokerList;
//    Set<>
//    Comparator<HostWithTime> timeComparator = new HostWithTimeComparator();
    // Map<Broker,Pair<Topic,Partition>>

    final int MONITOR_CLUSTER_INTERVAL = 30000;


    // min heap round robin timestamp queue
    // assign brokers when new producer apply a new topic
//    PriorityQueue<Pair<Timestamp,Broker>> clusters;

//    Map<String, List<Broker>> topicsMember;
//
//    // 作为coordinator要用到的讯息
//    Map<String, Broker> topics_coordinator;
//
//    // each topic's consumer group leader
//    Map<String, Consumer> consumerLeader;
//    Map<Consumer, Integer> consumerOffset;




    // 记录consumer， offset
    Map<String, Pair<Integer,Broker>> topic_map;

    // 接收來自producer的create_topic
    // 回傳這個topic, partition的負責人給傳的那個人
    public Zookeeper(String host, int port) throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.host = host;
        this.port = port;
        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);
//        clusters = new PriorityQueue<>((p1, p2) -> p1.getKey().compareTo(p2.getKey()));
        topicAssignmentHash = new HashMap<>() ;
//        replicationsHash = new HashMap<>();
        replicationHash = new HashMap<>();
        brokerList = new HashSet<>();
        topicBrokerQueue = new LinkedList<>();
        coordinatorBrokerQueue = new LinkedList<>();
        replicationBrokerQueue = new LinkedList<>();
//        updateClusterInterval = 5000;
        coordinatorAssignmentHash = new HashMap<>();
//        coordinatorAssignment("1111");
//        BrokerList
//        Set<HostRecord>
        this.tempBrokerList = new HashSet<>();
        this.brokerToTopicPartitionHash = new HashMap<>();

//
//        HostRecord ha = new HostRecord("localhost", 1 );
//        HostRecord haha = new HostRecord("localhost", 2 );
//        HostRecord hahaha = new HostRecord("localhost",3 );
//        HostRecord hahahaha = new HostRecord("localhost",4 );
//
//        topicBrokerQueue.add(ha);
//        replicationBrokerQueue.add(ha);
//        brokerList.add(ha);
//        topicBrokerQueue.add(haha);
//        replicationBrokerQueue.add(haha);
//        brokerList.add(haha);
////        topicBrokerQueue.add(hahaha);
////        replicationBrokerQueue.add(hahaha);
////        topicBrokerQueue.add(hahahaha);
////        replicationBrokerQueue.add(hahahaha);
//
//        Topic one = new Topic("hahaha");
//        one.replication = 2;
//        one.partition = 3;
//        HostRecord temp = new HostRecord("localhost", 9004);
////        createTopicAssignment(one);

//        topicAssignment(one, temp);


//        System.out.println(topicBrokerQueue.peek().getBorkerInfo().getPort().toString()+topicBrokerQueue.poll().getTimeStamp().toString());
//        System.out.println(topicBrokerQueue.peek().getBorkerInfo().getPort().toString()+topicBrokerQueue.poll().getTimeStamp().toString());
//        System.out.println(topicBrokerQueue.peek().getBorkerInfo().getPort().toString()+topicBrokerQueue.poll().getTimeStamp().toString());


//        tashMap();
////        topics_coordinator = new HashMap();
////        consumerLeader = new HashMap();
////        consumerOffset = new HashMap();opicsMember = new H

    }
//    public void monitorCluster(HostRecord oneBrokerHeart)
//    {
//
//
//
//
//    }

    public void reAssignLeader(HostRecord oneLeader){

    }
    // Yao-Chia

//    public void informCoordinator(HostRecord brokenBroker) {
////        HostRecord broken = new HostRecord();
////        HostRecord newAssigned
//        //Map<HostRecord,Set< Pair<String,Integer> > > brokerTopicPartionMap
//        Set<Pair<String,Integer>> topicPartitionPairs = brokerTopicPartionMap.get(brokenBroker);
//
//        List<Object> argument = new ArrayList<>();
//        Message request = new Message(MessageType.REPLACE_BROKER, argument);
//        // Coordinator 要的資料是 Topic -> Partition -> <Broken,New>
//
//    }
//
    public void monitorCluster() throws InterruptedException {
        //Set<HostRecord> brokerList;
        System.out.println("brokerList.size() = " + brokerList.size());
        while (true) {

            tempBrokerList = new HashSet<>(brokerList);
            System.out.println("tempBrokerList.size() = " + tempBrokerList.size());
            Thread.sleep(MONITOR_CLUSTER_INTERVAL);
            System.out.println("tempBrokerList.size() = " + tempBrokerList.size());
            if ( !tempBrokerList.isEmpty() ) {
                Map< String, Map< Integer,Pair<HostRecord,HostRecord> > > newAssignment = new HashMap<>();
                for ( HostRecord h: tempBrokerList) {
                    // B -> T-P
                    brokerList.remove(h);
                    Map<String, Integer> brokenBrokerTopicPartitionMap = brokerToTopicPartitionHash.get(h);
                    for ( Map.Entry<String, Integer> pair : brokenBrokerTopicPartitionMap.entrySet()) {
                        System.out.println(pair.getKey() + " " + pair.getValue());
                    }

                }
            }
        }
    }

    public void updateCluster(HostRecord healthyHeartBeat) {
        System.out.println("Zookeeper says hi! " + healthyHeartBeat.getPort());
        synchronized (this) {
            tempBrokerList.remove(healthyHeartBeat);
        }
    }

    // Yao-Chia

    //public void monitorCluster(HostRecord oneBrokerHeart)
    //{

//        if(replicationHash.containsKey(oneLeader))
//        {
//            for(HashMap<Pair<String, Integer>, HashSet<HostRecord>> oneLeaderContent: replicationHash.get(oneLeader))
//            {
//
//            }
//
//        }

//       for()
//        for(Map<Integer, Map<HostRecord, Set<HostRecord>>> oneTopicContent : replicationHash )
//        {
//            for(Map<HostRecord, Set<HostRecord>>) onePartitionContent : oneTopicContent)
//            {
//                if(onePart)
//            }
//        }
//            Map<String, Map<Integer, Set<HostRecord>>> temp = replicationHash.get(oneLeader);
//
//        HashMap<HostRecord, HashSet<HostRecord>> LeaderFollowers

    //}




    public Message coordinatorAssignment(String groupID){

        System.out.println("COORDINATORASSIGNMENT");
        if(!coordinatorAssignmentHash.containsKey(groupID))
        {
//                System.out.println("COORDINATORASSIGNMENT SUCCESS");
                assignCoordinator(groupID);
        }
        HostRecord temp = coordinatorAssignmentHash.get(groupID);
        System.out.println("COORDINATORASSIGNMENT SUCCESS");
        List<Object> arguments = new ArrayList();
        arguments.add(groupID);
        arguments.add(temp);
        Message response = new Message(MessageType.COORDINATOR_ASSIGNMENT, arguments);
        return response;
    }

    public void assignCoordinator(String groupID) {
        HostRecord tempBroker= coordinatorBrokerQueue.poll();
        coordinatorAssignmentHash.put(groupID, tempBroker);
        topicBrokerQueue.add(tempBroker);

    }



    public void createTopicAssignment(Topic topic) {
        System.out.println("TOPICASSIGNMENT");
//        topic.name = "HelloWorld";

        for(Integer i =0; i< topic.partition; i++)
        {
            HostRecord tempBroker= topicBrokerQueue.poll();
            if(!topicAssignmentHash.containsKey(topic.getName()))
            {
                HashMap<Integer, HostRecord> h = new HashMap<>();
                h.put(i, tempBroker);
                topicAssignmentHash.put(topic.getName(), h);
            }
            else
            {
                topicAssignmentHash.get(topic.getName()).put(i, tempBroker);
            }
//            TimeUnit.SECONDS.sleep(1);
//            tempBroker.setTimeStamp(new Date().getTime());
            topicBrokerQueue.add(tempBroker);
        }
//        System.out.println("CREATE TOPICASSIGNMENT Success");
//        System.out.println("Topic: " + topic.getName());
//        System.out.println();
//        displayTopicAssignment(topic.getName());
//        List<Object> arguments = new ArrayList();
//        arguments.add(topic);
//        arguments.add(topicAssignmentHash.get(topic.getName()));
//        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
//        response.setIsAck(true);
//        return response;
//        return response;
    }
    public Message topicPartitions(String topic)
    {
        List<Object> arguments = new ArrayList();
        if(!topicAssignmentHash.containsKey(topic))
        {
            System.out.println("Don't have the topic for consumer");
            arguments.add("ZooKeeper Don't have the topic: "+topic);
            Message response = new Message(MessageType.ACK, arguments,true);
            return response;

        }
        System.out.println("TOPICASSIGNMENT for Consumer");
        System.out.println("Topic: "+topic);
        System.out.println();
        displayTopicAssignment(topic);


        arguments.add(topic);
        arguments.add(topicAssignmentHash.get(topic));
        Message response = new Message(MessageType.RETURN_TOPIC_FOR_COORDINATOR, arguments);
        return response;
//        ("updateTopicPartitionLeaderCache")
//        public void updateTopicsPartitionLeader(String topic, Map<Integer, HostRecord> topicPartitionLeaders)
    }

    public Message topicAssignment(Topic topic, HostRecord sender) throws InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {

        if(!topicAssignmentHash.containsKey(topic.getName()))
        {
            createTopicAssignment(topic);
        }
        System.out.println("TOPICASSIGNMENT for producer");
        System.out.println("Topic: "+topic.getName());
        System.out.println();



        displayTopicAssignment(topic.getName());

        leaderReplicasAssignment(topic);
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(topicAssignmentHash.get(topic.getName()));
        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
        TcpClient sock = new TcpClient(sender.getHost(), sender.getPort());
        sock.setHandler( this, response );
        sock.run();

        leaderReplicasAssignment(topic);
        return response;

////        TcpClient sock = new TcpClient(sender.getHost(), sender.getPort());
//        sock.setHandler( this, response  );
//        sock.run();
//        leaderReplicasAssignment(topic);
//        return response;




//        response.setIsAck(true);
//        return response;
//        return topicAssignmentToBroker();
    }


    public void leaderReplicasAssignment(Topic topic) throws IOException, InvocationTargetException, NoSuchMethodException, InterruptedException, IllegalAccessException {

        System.out.println("ReplicationAssignment...");
//        topic.name = "HelloWorld";
        HashMap<Integer, HostRecord> partitions = topicAssignmentHash.get(topic.getName());
        for (Integer partition: partitions.keySet()){
            HashSet temp = new HashSet();
            int repCount = min(replicationBrokerQueue.size(),topic.replication);
            for(Integer i=0; i<repCount-1;i++) {

                HostRecord tempRep = topicBrokerQueue.poll();
                if(!tempRep.equals(partitions.get(partition)))
                    temp.add(tempRep);
                else
                    i--;
                topicBrokerQueue.add(tempRep);
            }
            if(!replicationHash.containsKey(topic.getName()))
            {
                replicationHash.put(topic.getName(), new HashMap<>());
            }
//            replicationHash.get(partitions.get(partition)).put(new Pair(topic.getName(), partition), temp);
            if(!replicationHash.get(topic.getName()).containsKey(partition))
            {
                replicationHash.get(topic.getName()).put(partition, new HashMap<>());
            }
            replicationHash.get(topic.getName()).get(partition).put(partitions.get(partition), temp);

            List<Object> arguments = new ArrayList();
            arguments.add(topic.getName());
            arguments.add(partition);
            arguments.add(partitions.get(partition));
//            arguments.add(replicationHash.get(partitions.get(partition)).get(new Pair(topic.getName(), partition)));
            arguments.add(replicationHash.get(topic.getName()).get(partition).get(partitions.get(partition)));
            Message assignment = new Message(MessageType.SET_TOPIC_PARTITION_LEADER, arguments);



            TcpClient sock = new TcpClient(partitions.get(partition).getHost(), partitions.get(partition).getPort());
            sock.setHandler( this, assignment );
            sock.run();

            String key =partition.toString();
            partitions.get(partition).toString();
            System.out.println("\nPartition " +key.toString() + "\n=Leader=\n" + partitions.get(partition).getHost() + " "+ partitions.get(partition).getPort());
            System.out.println("=Followers=");
            for(HostRecord oneFollower : replicationHash.get(topic.getName()).get(partition).get(partitions.get(partition)))
            {
                System.out.println(oneFollower.getHost() + " "+ oneFollower.getPort());
            }
        }
//        replicationsHash.get(partitions.get(partition)).get(topic.getName()).put(partition, )
    }
//    public void sendReplicasAssignment(Topic topic)
//    {
//        if()
//
//
//
//    }
    public void displayRelicationsHash(String topic)
    {


//
//        HashMap<Integer, HostRecord> partitions = topicAssignmentHash.get(topic);
//        for (Integer name: partitions.keySet()){
//
//            String key =name.toString();
//            partitions.get(name).toString();
//            System.out.println("Partition " +key.toString() + "  at" + partitions.get(name).getHost() + " "+ partitions.get(name).getPort());
//        }

    }


    public void displayTopicAssignment(String topic)
    {

        HashMap<Integer, HostRecord> partitions = topicAssignmentHash.get(topic);
        for (Integer name: partitions.keySet()){

            String key =name.toString();
            partitions.get(name).toString();
            System.out.println("Partition " +key.toString() + "  at" + partitions.get(name).getHost() + " "+ partitions.get(name).getPort());
        }

    }

    public Message topicAssignmentToBroker() {
        List<Object> arguments = new ArrayList<>();
        String temp = "Sucesss ";
        arguments.add(temp);
        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
        response.setIsAck(true);
        return response;
    }




    public Message newBrokerRegister(HostRecord oneBroker) {
        List<Object> arguments = new ArrayList();
        System.out.println(brokerList.size());
        if (!containsBroker(oneBroker)) {
            brokerList.add(oneBroker);
            coordinatorBrokerQueue.add(oneBroker);
            topicBrokerQueue.add(oneBroker);
            replicationBrokerQueue.add(oneBroker);
            System.out.println("Broker register completed");   //如果在這掛掉怎辦
//        arguments.add(coordinator);
            String temp = "Register Completed ACK";
            arguments.add(temp);
            displayBrokerList();
            Message response = new Message(MessageType.ACK, arguments, true);

            System.out.println(brokerList.size());
//            response.setIsAck(true);
            return response;
        }
        String temp = "Already Registered";
        arguments.add(temp);
        Message response = new Message(MessageType.ACK, arguments, true);
//        response.setIsAck(true);
        return response;
    }



    public void displayBrokerList(){
        for(HostRecord aBroker: brokerList)
        {
            System.out.println(aBroker.getHost()+" "+aBroker.getPort());
        }
    }



    public boolean containsBroker(HostRecord oneBroker)
    {
        for(HostRecord aBroker : brokerList)
        {
            if(aBroker.getHost().equalsIgnoreCase(oneBroker.getHost()) && aBroker.getPort().equals(oneBroker.getPort())) {
                return true;
            }
        }
        return false;
    }

    public void updateCluster() {
        // 新建broker





    }

    public void registerTopic() {
        // socket programming server接資料

        // assign broker and partition

        // communicate to the relative brokers

    }

    // 某個broker來問的
    public List<Pair<Integer,Broker>> responseTopicPartitionLeader(String topic) {
        List<Pair<Integer, Broker>> partitions = new ArrayList();
        // pair of partition and Broker
        return partitions;
    }

    public void listen() throws IOException, ClassNotFoundException, InterruptedException {

        listenSocket.listen();
//         while (true) {
//            // Send hearbeat per 1 min
//            Thread.sleep(updateClusterInterval);
//            try {
//                updateCluster();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InvocationTargetException e) {
//                e.printStackTrace();
//            } catch (NoSuchMethodException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (IllegalAccessException e) {
//                e.printStackTrace();
//            }
//        }
    }

    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 2181);
        z.listen();
//        Thread.sleep(1000000);
//        z.monitorCluster();
    }
}

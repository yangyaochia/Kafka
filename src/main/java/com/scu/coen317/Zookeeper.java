package com.scu.coen317;

import javafx.util.Pair;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.sql.Timestamp;
import java.util.*;


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
//    Comparator<HostWithTime> timeComparator = new HostWithTimeComparator();




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

    List<HostRecord> brokerList;



    // 记录consumer， offset
    Map<String, Pair<Integer,Broker>> topic_map;

    // 接收來自producer的create_topic
    // 回傳這個topic, partition的負責人給傳的那個人
    public Zookeeper(String host, int port) throws IOException, InterruptedException {
        this.host = host;
        this.port = port;
        this.listenSocket = new TcpServer(port);
        listenSocket.setHandler(this);
//        clusters = new PriorityQueue<>((p1, p2) -> p1.getKey().compareTo(p2.getKey()));
        topicAssignmentHash = new HashMap<>(100) ;

        topicBrokerQueue = new LinkedList<>();
        coordinatorBrokerQueue = new LinkedList<>();
        HostRecord ha = new HostRecord("localhost", 1 );
        HostRecord haha = new HostRecord("localhost", 2 );
        HostRecord hahaha = new HostRecord("localhost",3 );
        HostRecord hahahaha = new HostRecord("localhost",4 );

        topicBrokerQueue.add(ha);

        topicBrokerQueue.add(haha);

        topicBrokerQueue.add(hahaha);

        topicBrokerQueue.add(hahahaha);

//        Topic one = new Topic("hahahahah");
//        topicAssignment(one);


//        System.out.println(topicBrokerQueue.peek().getBorkerInfo().getPort().toString()+topicBrokerQueue.poll().getTimeStamp().toString());
//        System.out.println(topicBrokerQueue.peek().getBorkerInfo().getPort().toString()+topicBrokerQueue.poll().getTimeStamp().toString());
//        System.out.println(topicBrokerQueue.peek().getBorkerInfo().getPort().toString()+topicBrokerQueue.poll().getTimeStamp().toString());


//        tashMap();
////        topics_coordinator = new HashMap();
////        consumerLeader = new HashMap();
////        consumerOffset = new HashMap();opicsMember = new H
        brokerList = new ArrayList();
    }

    public void coordinatorAssignment(String groupID) throws  InterruptedException {
        System.out.println("COORDINATORASSIGNMENT");
        if(!coordinatorAssignmentHash.containsKey(groupID))
        {
                assignCoordinator(groupID);
        }
        HostRecord temp = coordinatorAssignmentHash.get(groupID);
        System.out.println("COORDINATORASSIGNMENT SUCCESS");
        List<Object> arguments = new ArrayList();
        arguments.add(temp);
        Message response = new Message(MessageType.COORDINATOR_ASSIGNMENT, arguments);
        response.setIsAck(true);
//        return response;

    }
    public void assignCoordinator(String groupID) {
        HostRecord tempBroker= coordinatorBrokerQueue.poll();
        coordinatorAssignmentHash.put(groupID, tempBroker);
        topicBrokerQueue.add(tempBroker);
        return;
    }



    public void createTopicAssignment(Topic topic, Integer partitionNum) {
        System.out.println("TOPICASSIGNMENT");
//        topic.name = "HelloWorld";

        for(Integer i =0; i< partitionNum; i++)
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

    public Message topicAssignment(Topic topic, Integer partitionNum) throws InterruptedException {

        if(!topicAssignmentHash.containsKey(topic.getName()))
        {
            createTopicAssignment(topic, partitionNum);
        }
        System.out.println("TOPICASSIGNMENT");
        System.out.println("Topic: "+topic.getName());
        System.out.println();
        displayTopicAssignment(topic.getName());
        List<Object> arguments = new ArrayList();
        arguments.add(topic);
        arguments.add(topicAssignmentHash.get(topic.getName()));
        Message response = new Message(MessageType.TOPIC_ASSIGNMENT_TO_BROKER, arguments);
//        response.setIsAck(true);
        return response;
//        return topicAssignmentToBroker();
    }

    public void displayTopicAssignment(String topic)
    {
        System.out.println("hash size : " + topicAssignmentHash.size());
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
        if (!containsBroker(oneBroker)) {
            brokerList.add(oneBroker);
            coordinatorBrokerQueue.add(oneBroker);
            topicBrokerQueue.add(oneBroker);
            System.out.println("Broker register completed");   //如果在這掛掉怎辦
//        arguments.add(coordinator);
            String temp = "Register Completed ACK ";
            arguments.add(temp);
            displayBrokerList();
            Message response = new Message(MessageType.REGISTER_SUCCESS, arguments);

//            response.setIsAck(true);
            return response;
        }
        String temp = "Register failed";
        arguments.add(temp);
        Message response = new Message(MessageType.REGISTER_SUCCESS, arguments);
        response.setIsAck(true);
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

    public void listen() throws IOException, ClassNotFoundException {

        listenSocket.listen();
    }

    public static void main(String argv[]) throws Exception {
        Zookeeper z = new Zookeeper("localhost", 2181);
        z.listen();
    }
}

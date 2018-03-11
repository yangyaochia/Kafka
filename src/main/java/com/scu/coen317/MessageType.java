package com.scu.coen317;

public enum MessageType {

    // Producer
    CREATE_TOPIC("getTopic"),                   // P -> B   Yaochia
    PUBLISH_MESSAGE("publishMessage"),              // P -> B   Yaochia

    // Broker
    GET_TOPIC("topicAssignment"),               // B -> Z   Y
    GET_TOPIC_FOR_COORDINATOR("topicPartitions"),   //B -> Z  coordinator ask zookeeper the topic's partitions
    TOPIC_ASSIGNMENT_TO_PRODUCER("updateTopicPartitionLeader"),        // B -> P   Y
    PUBLISH_MESSAGE_ACK("publishMessageAck"),       // B->P Y
    SUBSCRIBE_ACK("subscribeAck"),
    ACK(""),
    INITIAL_LEADER("initialLeader"),

    GET_REPLICATION_UPDATE("replicationResponse"),    // B -> B
    REPLICATION_RESPONSE("replicationUpdate"),
    SEND_HEARTBEAT("monitorCluster"),           // B -> Z
    NEW_BROKER_REGISTER("newBrokerRegister"),   // B -> Z Xinzhu
    GET_COORDINATOR("coordinatorAssignment"),   // B -> C      Xinzhu
    REBALANCE("rebalance"),            // B(Coordinator) -> C(Leader) : coordinator request rebalance Xinzhu
    REBALANCE_RESULT("updateTopicPartition"), // Coordinator send rebalance result, and send it <></>o each group member
    GIVE_MESSAGE("showMessageOut"), //B->C

    //ZooKeeper
    TOPIC_ASSIGNMENT_TO_BROKER("topicAssignmentToProducer"), //Z->B
    SET_TOPIC_PARTITION_LEADER("setTopicPartitionLeader"),  //Z->B follower
    SET_TOPIC_PARTITION_REPLICATION_HOLDER("setTopicPartitionReplicationHolder"),
    //send broker and tell it its new leader
    //if leader die, set a follower to be leader
    //if follower die, tell a new broker who is its leader
    REGISTER_SUCCESS("receiveNewBrokerRegistrationAck"), //Z->B
    REPLACE_BROKER("replaceTopicPartitionLeader"),  /*  Z -> B
                                                        zookeeper tell coordinator replace respectively topic partition leader
                                                        if any broker is dead
                                                     */
    COORDINATOR_ASSIGNMENT("updateCoordinator"), //Z->B
    RETURN_TOPIC_FOR_COORDINATOR("updateTopicsPartitionLeaderCache"), // Z -> B updateTopicsPartitionLeaderCache

    //Consumer
    FIND_COORDINATOR("getCoordinator"), //C->B  Xinzhu getCoordinator(String groupId)
    UPDATE_COORDINATOR("updateCoordinator"), // Xinzhu updateCoordinator(HostRecord coordinator)
    JOIN_GROUP("addConsumerToGroup"), //C->Bg
    SUBSCRIBE_TOPIC("storeInfoAndGetTopicAndRebalance"), //C->Bg //store who subscribe what topics and give to consumer leader later
    REBALANCEPLAN("updateBalanceMap"), //C1->Bg Xinzhu
    TEST1("test1"),
    TEST2("test2"),
    PULLMESSAGE("giveMessage"), //C->B
    SEND_MESSAGE_TO_CONSUMER("dealWithMessage");




    private String messageMame;
    private MessageType(String name) {
        this.messageMame = name;
    }

    @Override
    public String toString(){
        return messageMame;
    }
}

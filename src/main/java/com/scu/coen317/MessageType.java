package com.scu.coen317;

public enum MessageType {

    // Producer
    CREATE_TOPIC("getTopic"),                   
    PUBLISH_MESSAGE("publishMessage"),          

    // Broker
    GET_TOPIC("topicAssignment"),               
    GET_TOPIC_FOR_COORDINATOR("topicPartitions"),
    TOPIC_ASSIGNMENT_TO_PRODUCER("updateTopicPartitionLeader"),        
    PUBLISH_MESSAGE_ACK("publishMessageAck"),      
    SUBSCRIBE_ACK("subscribeAck"),
    ACK(""),
    INITIAL_LEADER("initialLeader"),
    GET_REPLICATION_UPDATE("replicationResponse"),   
    REPLICATION_RESPONSE("replicationUpdate"),
    SEND_HEARTBEAT("updateCluster"),           
    NEW_BROKER_REGISTER("newBrokerRegister"),  
    GET_COORDINATOR("coordinatorAssignment"),  
    REBALANCE("rebalance"),           
    REBALANCE_RESULT("updateTopicPartition"), 
    GIVE_MESSAGE("showMessageOut"), 
    TEST_REASSIGN("TEST_REASSIGN"),

    //ZooKeeper
    TOPIC_ASSIGNMENT_TO_BROKER("topicAssignmentToProducer"), 
    SET_TOPIC_PARTITION_LEADER("setTopicPartitionLeader"),  
    SET_TOPIC_PARTITION_REPLICATION_HOLDER("setTopicPartitionReplicationHolder"),
    REGISTER_SUCCESS("receiveNewBrokerRegistrationAck"), 
    REPLACE_BROKER("replaceTopicPartitionLeader"),  /* 
                                                     *  zookeeper tell coordinator replace respectively topic partition leader
                                                     *  if any broker is dead
                                                     */
    COORDINATOR_ASSIGNMENT("updateCoordinator"), 
    RETURN_TOPIC_FOR_COORDINATOR("updateTopicsPartitionLeaderCache"), 

    //Consumer
    FIND_COORDINATOR("getCoordinator"), 
    UPDATE_COORDINATOR("updateCoordinator"),
    JOIN_GROUP("addConsumerToGroup"), 
    SUBSCRIBE_TOPIC("storeInfoAndGetTopicAndRebalance"), 
    REBALANCEPLAN("updateBalanceMap"), 
    PULLMESSAGE("giveMessage"), 
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

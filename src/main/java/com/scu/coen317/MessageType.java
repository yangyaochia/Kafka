package com.scu.coen317;

public enum MessageType {
    // SENDER_ACTION("RECEIVER_ACTION")
    // Producer
//    CREATE_TOPIC("getTopic"),                   // P -> B
//    PUB_MESSAGE("publishMessage"),              // P -> B
//    // Broker
//    GET_TOPIC("topicAssignment"),               // B -> Z
//    TOPIC_ASSIGNMENT_TO_PRODUCER("publish_Message"),        // B -> P
//    GET_REPLICATION_UPDATE("replicationResponse"),    // B -> B
//    REPLICATION_RESPONSE("replicationUpdate"),
//    SEND_HEARTBEAT("monitorCluster"),           // B -> Z
//    NEW_BROKER_REGISTER("newBrokerRegister"),   // B -> Z
//    GET_COORDINATOR("coordinatorAssignment"),
//    CONSUMER_JOIN_GROUP_REGISTRATION_ACK("receiveConsumerJoinGroupRegistrationAck"),
//    REBALANCE("rebalance"),            // B(Coordinator) -> C(Leader) : coordinator request rebalance
//
//    UPDATE_TOPIC_PARTITION("updateTopicPartition"),// Coordinator send rebalance result, and send it to each group member
//
//
//
//    SEND_MESSAGE("receivedMessage"),
//    SEND_MESSAGE_ACK("receivedMessageAck");

    // Producer
    CREATE_TOPIC("getTopic"),                   // P -> B   Y
    PUBLISH_MESSAGE("publishMessage"),              // P -> B   Y

    // Broker
    GET_TOPIC("topicAssignment"),               // B -> Z   Y
    TOPIC_ASSIGNMENT_TO_PRODUCER("publish_Message"),        // B -> P   Y
    PUBLISH_MESSAGE_ACK("publishMessageAck"),       // B->P Y
    GET_REPLICATION_UPDATE("replicationResponse"),    // B -> B
    REPLICATION_RESPONSE("replicationUpdate"),
    SEND_HEARTBEAT("monitorCluster"),           // B -> Z
    NEW_BROKER_REGISTER("newBrokerRegister"),   // B -> Z
    GET_COORDINATOR("coordinatorAssignment"),
    CONSUMER_JOIN_GROUP_REGISTRATION_ACK("receiveConsumerJoinGroupRegistrationAck"),
    REBALANCE("rebalance"),            // B(Coordinator) -> C(Leader) : coordinator request rebalance
    REBALANCE_RESULT("pullMessage"), // Coordinator send rebalance result, and send it <></>o each group member
    GIVE_MESSAGE("showMessageOut"), //B->C

    //ZooKeeper
    TOPIC_ASSIGNMENT_TO_BROKER("topicAssignmentToProduer"), //Z->B
    SET_TOPIC_LEADER("setTopicLeader"),  //Z->B follower
    //send broker and tell it its new leader
    //if leader die, set a follower to be leader
    //if follower die, tell a new broker who is its leader
    REGISTER_SUCCESS("receiveNewBrokerRegistrationAck"), //Z->B
    COORDINATOR_ASSIGNMENT("coordinatorAssignmentToConsumer"), //Z->B

    //Consumer
    FIND_COORDINATOR("getCoordinator"), //C->B
    JOIN_GROUP("addConsumerToGroup"), //C->Bg
    SUBSCRIBE_TOPIC("storeInfoAndGetTopic"), //C->Bg //store who subscribe what topics and give to consumer leader later
    REBALANCEPLAN("assignByRebalancePlan"), //C1->Bg
    PULLMESSAGE("giveMassage"); //C->B


    private String messageMame;
    private MessageType(String name) {
        this.messageMame = name;
    }

    @Override
    public String toString(){
        return messageMame;
    }
}

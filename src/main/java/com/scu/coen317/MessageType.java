package com.scu.coen317;

public enum MessageType {

    CREATE_TOPIC("createTopic"),
    SEND_MESSAGE("receivedMessage"),
    SEND_MESSAGE_ACK("receivedMessageAck");

    private String messageMame;
    private MessageType(String name) {
        this.messageMame = name;
    }

    @Override
    public String toString(){
        return messageMame;
    }
}

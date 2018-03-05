package com.scu.coen317;

public enum MessageType {

    CREATE_TOPIC("createTopic"),
    SEND_MESSAGE("sendMessage");


    private String messageMame;
    private MessageType(String name) {
        this.messageMame = name;
    }

    @Override
    public String toString(){
        return messageMame;
    }
}

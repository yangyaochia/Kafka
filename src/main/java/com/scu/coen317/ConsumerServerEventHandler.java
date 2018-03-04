package com.scu.coen317;

public class ConsumerServerEventHandler implements TcpServerEventHandler{
    Consumer consumer;
    public ConsumerServerEventHandler(Consumer consumer) {
        this.consumer = consumer;
    }
    @Override
    public void onMessage(int client_id, String line) {

    }

    @Override
    public void onAccept(int client_id) {

    }

    @Override
    public void onClose(int client_id) {

    }
}

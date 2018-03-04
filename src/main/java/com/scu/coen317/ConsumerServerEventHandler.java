package com.scu.coen317;

import java.util.List;

public class ConsumerServerEventHandler implements TcpClientEventHandler{
    Consumer consumer;
    public ConsumerServerEventHandler(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onMessage(List<Object> msg) {
        System.out.println("consumer handler");
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClose() {

    }
}

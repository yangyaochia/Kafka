package com.scu.coen317;

import java.util.List;

public class ConsumerClientEventHandler implements TcpClientEventHandler {
    Consumer consumer;
    public ConsumerClientEventHandler(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onMessage(List<Object> msg) {
        consumer.helloWord = "Received request";
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClose() {

    }
}

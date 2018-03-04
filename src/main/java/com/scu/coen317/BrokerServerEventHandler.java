package com.scu.coen317;

import java.util.List;

public class BrokerServerEventHandler implements TcpClientEventHandler {
    Broker broker;

    public BrokerServerEventHandler(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void onMessage(List<Object> msg) {
        if(Integer.parseInt((String)msg.get(0)) == 1) {
            sendCoordinator();
        }
    }

    public void sendCoordinator() {

    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClose() {

    }
}

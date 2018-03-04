package com.scu.coen317;

public class MyRunnable implements Runnable {
    BrokerClientEventHandler brokerClientEventHandler;

    public MyRunnable(BrokerClientEventHandler h) {
        this.brokerClientEventHandler = h;
    }

    @Override
    public void run() {

    }
}

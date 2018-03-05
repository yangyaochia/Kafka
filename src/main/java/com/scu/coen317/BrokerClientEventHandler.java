package com.scu.coen317;

import java.util.List;

public class BrokerClientEventHandler implements TcpClientEventHandler{
    private Broker broker;

    public BrokerClientEventHandler(Broker b) {
        this.broker = b;
    }
    @Override
    public void onMessage(List<Object> msg) {

        /*for ( int i = 0 ; i < msg.size() ; i++ ) {
            Object obj = msg.get(i);
            if ( obj instanceof Topic ) {
                //System.out.println( ((Topic)obj).getName() );
            } else {
                //System.out.println(obj);
                broker.temp[]
            }
        }*/
        broker.temp.put((String)msg.get(0), (String)msg.get(1));
        broker.printTemp();
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClose() {

    }
}

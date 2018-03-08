package com.scu.coen317;

public class BrokerTestYaoChia {
    public static void main(String argv[]) throws Exception {
        Broker b1 = new Broker("localhost", 9000, "localhost", 2181);
        Broker b2 = new Broker("localhost", 9001, "localhost", 2181);
        Broker b3 = new Broker("localhost", 9002, "localhost", 2181);
        b1.listen();
        b2.listen();
        b3.listen();
        Broker xinzhuBroker = new Broker("localhost", 9005, "localhost", 2181);
        xinzhuBroker.updateCoordinator("group1", xinzhuBroker.thisHost);
        xinzhuBroker.listen();
    }
}

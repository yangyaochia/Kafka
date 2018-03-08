package com.scu.coen317;

public class BrokerTestYaoChia {
    public static void main(String argv[]) throws Exception {
        Broker b = new Broker("localhost", 9000, "localhost", 2181);
        b.listen();

    }
}

package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TestBroker2 {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, ClassNotFoundException, InterruptedException {
        Broker broker2 = new Broker("localhost", 9006, "localhost", 2181);
        broker2.registerToZookeeper();
        broker2.listen();

        TcpClient test = new TcpClient("localhost", 9005);
        HostRecord temp = new HostRecord("localhost", 9005);
        List<Object> arguments = new ArrayList();
        arguments.add("group1");
        arguments.add(temp);
        Message response = new Message(MessageType.COORDINATOR_ASSIGNMENT, arguments);
        test.setHandler(broker2,response);
        test.run();

    }
}

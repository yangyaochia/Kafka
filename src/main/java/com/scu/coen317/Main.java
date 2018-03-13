package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.scu.coen317.Broker;

public class Main {
    public static void main(String[] args) {
        // Broker localhost 9000 localhost 2181
        // Consumer localhost 10001 group1 localhost 9005
        // Producer localhost 8000 localhost 9000 2 2
        // Zookeeper localhost 2181

        String host = "localhost";
        int portNumber = Integer.parseInt(args[2]);
        int defautPort = 9005;
        if (args.length < 3) {
            System.out.println("Error : Illegal input.");
        } else
            try {
                if (args[0].equals("Zookeeper")) {
                    Zookeeper zookeeper = new Zookeeper(host, portNumber);
                    zookeeper.listen();
                    zookeeper.monitorCluster();
                } else if (args[0].equals("Broker")) {
                    Broker broker = new Broker(args[1],
                            Integer.parseInt(args[2]), args[3], Integer.parseInt(args[4]));
                    broker.registerToZookeeper();
                    broker.listen();
                    broker.sendHeartBeat();
                } else if (args[0].equals("Producer1")) {
                    Producer p = new Producer("localhost",
                            Integer.parseInt(args[1]), "localhost", Integer.parseInt(args[2]));
                    p.addDefaultBroker("localhost", 9001);
                    p.addDefaultBroker("localhost", 9002);
                    p.createTopic(args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]));
                    for (int i = 0; i < 1500; i++) {
                        p.publishMessage(args[3], Integer.toString(i));
                    }
                } else if (args[0].equals("Consumer")) {

                    Consumer consumer = new Consumer(args[1], Integer.parseInt(args[2]),
                            args[3], args[4], Integer.parseInt(args[5]));

                        consumer.findCoordinator();
                        consumer.joinToGroup();
                        consumer.subscribe(args[6]);
                        if (args.length == 8) {
                            consumer.subscribe(args[7]);
                        consumer.poll();
                    }

                }
            } catch (IOException e) {
//                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (InterruptedException e) {
//                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (NoSuchMethodException e) {
//                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (IllegalAccessException e) {
//                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (InvocationTargetException e) {
//                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (ClassNotFoundException e) {
//            e.printStackTrace();
            }
    }

}

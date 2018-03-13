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
                } else if (args[0].equals("Producer")) {
                    String topic = args[5];
                    Producer p = new Producer(args[1],
                            Integer.parseInt(args[2]), args[3], Integer.parseInt(args[4]));
                    p.addDefaultBroker("localhost", 9001);
                    p.addDefaultBroker("localhost", 9002);
                    p.createTopic(topic, Integer.parseInt(args[6]), Integer.parseInt(args[7]) );
                    for (int i = 0; i < 1500; i++) {
                        p.publishMessage(topic, "Topic: Message from " +args[2]+ " " + Integer.toString(i));
                    }
//                    p.createTopic("Distributed System Topic", 3,1);
//                    for ( int i = 0 ; i < 1500 ; i++) {
//                        p.publishMessage(topic, "DS Topic: Message from 8000 " + Integer.toString(i));
//                    }
                } else if (args[0].equals("Consumer")) {

                    String topic1 = args[6];
                    System.out.println(topic1);
                    String topic2 = args.length == 8 ? args[7] : "";
                    Consumer consumer = new Consumer(host, Integer.parseInt(args[2]),
                            args[3], host, Integer.parseInt(args[5]));

                    consumer.findCoordinator();
                    consumer.joinToGroup();
                    consumer.subscribe(topic1);
                    if (args.length == 8) {
                        consumer.subscribe(topic2);
                    }
                    consumer.poll();


//                    Consumer con1 = new Consumer("localhost", 10001, "group1",
//                            "localhost", 9000);
//                    con1.findCoordinator();
////        con1.updateCoordinator(new HostRecord("localhost", 9005));
//                    con1.joinToGroup();
//                    con1.subscribe("DS");
////        Thread.sleep(1000);
//                    con1.poll();


                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (InvocationTargetException e) {
                e.printStackTrace();
                System.out.println("Error : Illegal input.");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
    }

}

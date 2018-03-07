package com.scu.coen317;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TcpServer {
    private ServerSocket server;
    private TcpServerEventHandler handler;

    private ArrayList<TcpClient> clients;

    public ArrayList<TcpClient> getClients() {
        return clients;
    }

    private boolean closer = false;
    private int readInterval;

    public int getReadInterval() {
        return readInterval;
    }

    public void setReadInterval(int msec) {
        this.readInterval = msec;
        for (TcpClient sock : clients) {
            sock.setReadInterval(msec);
        }
    }

    public TcpServer(int port) {
        if (server != null && !server.isClosed()) return;
        if (clients == null) clients = new ArrayList<TcpClient>();
        try {
            server = new ServerSocket(port);
        } catch (Exception ex) {
        }
    }

    public void listen() {
        new Thread() {
            public void run() {
                while (!closer) {
                    try {
                        TcpClient sock = new TcpClient(server.accept());

                        clients.add(sock);
                        final int cid = clients.size() - 1; // client id
                        handler.onAccept(cid);
                        if (handler != null) {
                            sock.addEventHandler(new TcpClientEventHandler() {
                                public void onMessage(Message message) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {
                                    handler.onMessage(cid, message);
                                }

                                public void onOpen() {
                                }

                                public void onClose() {
                                    handler.onClose(cid);
                                }
                            });
                        }
                        sock.run();
                    } catch (Exception ex) {
                    }
                }
            }
        }.start();

//        new Thread(){
//            public void run(){
//                while(true){
//                    try{
//                        Thread.sleep(20000);
//                    }
//                    catch(Exception ex){
//                    }
//                    for(TcpClient sock : clients){
//                        sock.send(Collections.singletonList(""));
//                    }
//                }
//            }
//        }.start();
    }


    public void setHandler(Class clazz, Object object) {
        final TcpServer that_server = this;
        final Object this_object = object;
        this.handler = new TcpServerEventHandler() {
            public void onMessage(int client_id, Message message) throws InvocationTargetException, IllegalAccessException, IOException {
                System.out.println("进入server onMessage");
                Class<?>[] inputTypes = message.getInputParameterType();
                try {
                    Method method = clazz.getMethod(message.getMethodNameValue(), inputTypes);

                    Object[] inputs = message.getInputValue();
                    System.out.println("* <" + client_id + "> will invoke " + message.getMethodName());
                    Message response = (Message) method.invoke(this_object, inputs);


                //msg.add(0, "echo : <"+client_id+"> ");
                that_server.getClient(client_id).send(response);
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                    System.out.println("no such method");
                }
                System.out.println("* < send to " + client_id + "> " + "successful");
            }

            public void onAccept(int client_id) {
                System.out.println("* <" + client_id + "> connection accepted");
                that_server.setReadInterval(100 + that_server.getClients().size() * 10);
            }

            public void onClose(int client_id) {
                System.out.println("* <" + client_id + "> closed");
            }
        };
    }


    public TcpClient getClient(int id) {
        return clients.get(id);
    }

    public void close() {
        closer = true;
        try {
            server.close();
            for (TcpClient sock : clients) {
                sock.close();
            }
        } catch (Exception ex) {
        }
    }

    public void addEventHandler(TcpServerEventHandler serverHandler) {
        this.handler = serverHandler;
    }


}

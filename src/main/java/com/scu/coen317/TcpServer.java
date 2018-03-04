package com.scu.coen317;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.ConnectException;
import java.util.ArrayList;

public class TcpServer implements Runnable {
    private ServerSocket server;
    private TcpServerEventHandler serverHandler;
    private TcpClientEventHandler clientHandler;
    private ArrayList<TcpClient> clients;
    public ArrayList<TcpClient> getClients(){ return clients; }
    private boolean closer = false;
    private int readInterval;
    public int getReadInterval(){ return readInterval; }
    public void setReadInterval(int msec){
        this.readInterval = msec;
        for(TcpClient sock : clients){
            sock.setReadInterval(msec);
        }
    }

    public TcpServer(int port){
        if(server != null && !server.isClosed()) return;
        if(clients == null) clients = new ArrayList<TcpClient>();
        try{
            server = new ServerSocket(port);
        }
        catch(Exception ex){
        }
    }

    public void listen(){
        BrokerClientEventHandler h = new BrokerClientEventHandler();
        Runnable r = new MyRunnable(h) {
            @Override
            public void run() {
                while(!closer){
                    try{
                        TcpClient sock = new TcpClient(server.accept());
                        clients.add(sock);
                        final int cid = clients.size()-1; // client id
                        handler.onAccept(cid);
                        if(handler != null){
                            sock.addEventHandler(h);
                        }
                        sock.run();
                    }
                    catch(Exception ex){
                    }
                }
            }
        }

        new Thread(r).start();
//        new Thread(){
//            public void run(){
//                while(!closer){
//                    try{
//                        TcpClient sock = new TcpClient(server.accept());
//                        clients.add(sock);
//                        final int cid = clients.size()-1; // client id
//                        handler.onAccept(cid);
//                        if(handler != null){
//                            sock.addEventHandler(new TcpClientEventHandler(){
//                                public void onMessage(String line){
//                                    handler.onMessage(cid, line);
//                                }
//                                public void onOpen(){
//                                }
//                                public void onClose(){
//                                    handler.onClose(cid);
//                                }
//                            });
//                        }
//                        sock.run();
//                    }
//                    catch(Exception ex){
//                    }
//                }
//            }
//        }.start();

        new Thread(){
            public void run(){
                while(true){
                    try{
                        Thread.sleep(20000);
                    }
                    catch(Exception ex){
                    }
                    for(TcpClient sock : clients){
                        sock.send("");
                    }
                }
            }
        }.start();
    }

    public TcpClient getClient(int id){
        return clients.get(id);
    }

    public void close(){
        closer = true;
        try{
            server.close();
            for(TcpClient sock : clients){
                sock.close();
            }
        }
        catch(Exception ex){
        }
    }

    public void addEventServerHandler(TcpServerEventHandler handler){
        this.serverHandler = handler;
    }
    public void addEventClientHandler(TcpClientEventHandler handler){
        this.clientHandler = handler;
    }
}

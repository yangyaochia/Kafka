package com.scu.coen317;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;

public class TcpServer {
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
        new Thread(){
            public void run(){
                System.out.println("Listening");
                while(!closer){
                    try{
                        //System.out.println("Listening");
                        TcpClient sock = new TcpClient(server.accept());
                        System.out.println(sock);
                        clients.add(sock);
                        final int cid = clients.size()-1; // client id
                        //serverHandler.onAccept(cid);
                        System.out.println("Accpted!!!!");
                        if(serverHandler != null){
                            sock.addEventHandler(clientHandler);
                        }
                        System.out.println("Accpted!!!!");
                        //sock.connect();
                        sock.run();
                    }
                    catch(Exception ex){
                    }
                }
            }
        }.start();

        /*new Thread(){
            public void run(){
                while(true){
                    try{
                        Thread.sleep(20000);
                    }
                    catch(Exception ex){
                    }
                    for(TcpClient sock : clients){
                        sock.send(Collections.singletonList(""));
                    }
                }
            }
        }.start();*/
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

    public void addEventHandler(TcpServerEventHandler sHandler, TcpClientEventHandler cHandler){
        this.serverHandler = sHandler;
        this.clientHandler = cHandler;
    }
    public static void main(String argv[]) throws Exception {

        //b.receive_msg();
        TcpServer server = new TcpServer(9000);

// add event handler, response to client
        final TcpServer that_server = server;
//        server.addEventHandler(new TcpServerEventHandler(){
//            public void onMessage(int client_id, String line){
//                System.out.println("* <"+client_id+"> "+ line);
//                that_server.getClient(client_id).send("echo : <"+client_id+"> "+line);
//            }
//            public void onAccept(int client_id){
//                System.out.println("* <"+client_id+"> connection accepted");
//            }
//            public void onClose(int client_id){
//                System.out.println("* <"+client_id+"> closed");
//            }
//        });

        server.listen();

        //server.close();

    }

}

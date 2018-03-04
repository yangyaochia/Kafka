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

    // server会开一个client类型的socket给发送请求的client传送response
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
        this.setServerHandler();
    }

    public void listen(){
        new Thread(){
            public void run(){
                while(!closer){
                    try{
                        TcpClient sock = new TcpClient(server.accept());
                        System.out.println("Received socket");
                        clients.add(sock);
                        final int cid = clients.size()-1; // client id
                        serverHandler.onAccept(cid);
                        if(serverHandler != null){
                            sock.addEventHandler(clientHandler);
                        }
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

    public void addEventHandler(TcpClientEventHandler cHandler){
        this.clientHandler = cHandler;
    }

    public void setServerHandler() {
        final TcpServer that_server = this;
        this.serverHandler = new TcpServerEventHandler() {
            public void onMessage(int client_id, String line) {
                System.out.println("* <" + client_id + "> " + line);
                that_server.getClient(client_id).send("echo : <" + client_id + "> " + line);
            }

            public void onAccept(int client_id) {
                System.out.println("* <" + client_id + "> connection accepted");
            }

            public void onClose(int client_id) {
                System.out.println("* <" + client_id + "> closed");
            }
        };
    }

}

package com.scu.coen317;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.ConnectException;

public class TcpClient {
    private String host;
    private int port;
    private Socket sock;
    private int readInterval;
    public int getReadInterval(){ return readInterval; }
    public void setReadInterval(int msec){ this.readInterval = msec; }
//    private BufferedInputStream bReader;
    private ObjectInputStream inFromClient;
//    private BufferedReader bReader;

//    private BufferedOutputStream bWriter;
//    private BufferedWriter bWriter;
    private ObjectOutputStream outToServer;
    private TcpClientEventHandler handler;
    private boolean closer = false;

    public TcpClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.sock = new Socket(host,port);
        outToServer = new ObjectOutputStream(this.sock.getOutputStream());
        inFromClient = new ObjectInputStream(this.sock.getInputStream());
    }

    public TcpClient(Socket connected_socket){
        this.sock = connected_socket;
    }

    public boolean run(){
        this.closer = false;
        try{
            if ( inFromClient == null )
                this.inFromClient = new ObjectInputStream(this.sock.getInputStream());
            if ( outToServer == null )
                this.outToServer = new ObjectOutputStream(this.sock.getOutputStream());
        }
        catch(ConnectException ex){
            if(handler != null) handler.onClose();
            return false;
        }
        catch(Exception ex){
            this.close();
            if(handler != null) handler.onClose();
            return false;
        }
        final TcpClient that = this;
        new Thread(() -> {

            while(!closer){
                try{
                    Thread.sleep(readInterval);
                    Message message = (Message) inFromClient.readObject();
                    if(message != null ){
                        if(handler != null) handler.onMessage(message);
                    }

                }
                catch(SocketException ex){
                    that.close();
                }
                catch(IOException ex){
                    that.close();
                }
                catch(Exception ex){
                    that.close();
                }
            }
        }).start();
        if(handler != null) handler.onOpen();
        return true;
    }

    public boolean connect(){
        if(this.sock != null) return false;
        try{
            this.sock = new Socket(host, port);
        }
        catch(ConnectException ex){
            if(handler != null) handler.onClose();
            return false;
        }
        catch(Exception ex){
            this.close();
            if(handler != null) handler.onClose();
            return false;
        }
        return run();
    }

    public void close(){
        try{
            closer = true;
            inFromClient.close();
            outToServer.close();
            sock.close();
            sock = null;
            if(handler != null) handler.onClose();
        }
        catch(Exception ex){
        }
    }

    public boolean send(Message message){
        if(sock == null) return false;
        try{
            outToServer.writeObject(message);
        }
        catch(Exception ex){
            this.close();
            if(handler != null) handler.onClose();
            return false;
        }
        return true;
    }

    public void addEventHandler(TcpClientEventHandler handler){
        this.handler = handler;
    }
}

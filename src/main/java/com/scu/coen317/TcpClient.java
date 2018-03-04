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
    //private BufferedWriter bWriter;
    //private BufferedReader bReader;
    //private InputStreamReader iReader;
    private ObjectInputStream inFromClient;
    private ObjectOutputStream outToServer;
    private TcpClientEventHandler handler;
    private boolean closer = false;

    ObjectInputStream inFromClient = new ObjectInputStream(connectionSocket.getInputStream());



    public TcpClient(String host, int port){
        this.host = host;
        this.port = port;
    }

    public TcpClient(Socket connected_socket){
        this.sock = connected_socket;
    }

    public boolean run(){
        this.closer = false;
        try{
            this.bWriter = new BufferedWriter(new OutputStreamWriter(this.sock.getOutputStream()));
            this.iReader = new InputStreamReader(this.sock.getInputStream());
            this.bReader = new BufferedReader(this.iReader);
            this.outToServer = new ObjectOutputStream(sock.getOutputStream());
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
        new Thread(){
            public void run(){
                while(!closer){
                    try{
                        Thread.sleep(readInterval);
                        String line = bReader.readLine();
                        if(line != null && line.length() > 0){
                            if(handler != null) handler.onMessage(line);
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
            }
        }.start();
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
            bReader.close();
            bWriter.close();
            iReader.close();
            sock.close();
            sock = null;
            if(handler != null) handler.onClose();
        }
        catch(Exception ex){
        }
    }

    public boolean send(String line){
        if(sock == null) return false;
        try{
            bWriter.write(line+"\n");
            bWriter.flush();
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

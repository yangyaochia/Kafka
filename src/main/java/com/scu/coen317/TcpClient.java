package com.scu.coen317;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.ConnectException;
import java.util.List;

public class TcpClient {
    private String host;
    private int port;
    private Socket sock;
    private int readInterval;
    public int getReadInterval(){ return readInterval; }
    public void setReadInterval(int msec){ this.readInterval = msec; }
    private BufferedInputStream bReader;
    private ObjectInputStream inFromClient;

    private BufferedOutputStream bWriter;
    private ObjectOutputStream outToServer;
    private TcpClientEventHandler handler;
    private boolean closer = false;

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
            this.bReader = (BufferedInputStream) this.sock.getInputStream();//new BufferedInputStream(inFromClient);
            this.inFromClient = new ObjectInputStream(bReader);//(ObjectInputStream) this.sock.getInputStream();


            this.outToServer = (ObjectOutputStream) this.sock.getOutputStream();
            this.bWriter = new BufferedOutputStream(outToServer);

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
                    List<Object> message = (List<Object>) inFromClient.readObject();
                    handler.onMessage(message);
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
            bReader.close();
            bWriter.close();
            inFromClient.close();
            outToServer.close();
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
            bWriter.write(Integer.parseInt(line+"\n"));
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

package com.scu.coen317;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.ConnectException;
import java.util.Collections;
import java.util.List;

public class TcpClient {
    private String host;
    private int port;
    private Socket sock;
    private int readInterval;
    public int getReadInterval(){ return readInterval; }
    public void setReadInterval(int msec){ this.readInterval = msec; }
    private BufferedReader bReader;
    private ObjectInputStream inFromClient;

    private BufferedWriter bWriter;
    private ObjectOutputStream outToServer;
    private TcpClientEventHandler handler;
    private boolean closer = false;

    public TcpClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.sock = new Socket(host, port);
        outToServer = new ObjectOutputStream(this.sock.getOutputStream());
        inFromClient = new ObjectInputStream(this.sock.getInputStream());
    }

    public TcpClient(Socket connected_socket){
        this.sock = connected_socket;
    }

    public boolean run(){
        this.closer = false;
        try{
            if ( outToServer == null )
                outToServer = new ObjectOutputStream(this.sock.getOutputStream());
            if ( inFromClient == null )
                inFromClient = new ObjectInputStream(this.sock.getInputStream());
            //BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
//            this.bReader =  new BufferedReader(new InputStreamReader(this.sock.getInputStream()));
//                    //this.sock.getInputStream();//new BufferedInputStream(inFromClient);
//            this.inFromClient = new ObjectInputStream(bReader);//(ObjectInputStream) this.sock.getInputStream();
//
//
//            this.outToServer = (ObjectOutputStream) this.sock.getOutputStream();
//            this.bWriter = new BufferedOutputStream(outToServer);

        } catch(Exception ex){
            this.close();
            if(handler != null) handler.onClose();
            return false;
        }
        final TcpClient that = this;
        new Thread(() -> {
            while(!closer){
                try{
                    //Thread.sleep(readInterval);
                    List<Object> request = (List<Object>) inFromClient.readObject();
                    System.out.println("Receiving " + request.size());
                    for ( int i = 0 ; i < request.size() ; i++ ) {
                        Object obj = request.get(i);
                        if ( obj instanceof Topic ) {
                            System.out.println( ((Topic)obj).getName() );
                        } else {
                            System.out.println(obj);
                        }
                    }
                    //handler.onMessage(request);

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

    public boolean send(List<Object> message){
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
    public static void main(String argv[]) throws Exception {
        TcpClient sock = new TcpClient("localhost", 9000);

// add handler
        final TcpClient that_sock = sock;
//        sock.addEventHandler(new TcpClientEventHandler(){
//            public void onMessage(String line){
//                System.out.println(" > "+line);
//            }
//            public void onOpen(){
//                System.out.println("* socket connected");
//            }
//            public void onClose(){
//                System.out.println("* socket closed");
//            }
//        });
        //sock.connect();
        sock.send(Collections.singletonList("hello!!"));

        sock.close();

    }
}

package com.scu.coen317;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
    private ObjectInputStream inFromClient;
//    private BufferedReader bReader;

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

    public boolean run() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
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

    public boolean connect() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
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

    public void setHandler(Object object, Message request) {
        final TcpClient that_sock = this;
        final Object this_object = object;
        this.handler = new TcpClientEventHandler(){
            public void onMessage(Message msg) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
                //handler.onMessage(cid, msg);
                System.out.println("进入client的handler");
                if ( msg.isAck() ) {
                    System.out.println(msg.getMethodNameValue());
                    that_sock.close();
                } else {

                    Class<?>[] inputTypes = msg.getInputParameterType();
                    Method method = object.getClass().getMethod(msg.getMethodNameValue(), inputTypes);
                    Object[] inputs = msg.getInputValue();
                    method.invoke(this_object, inputs);

                    System.out.println(msg.getMethodName());
                }
            }

            public void onOpen() {
                System.out.println("* socket connected");
                int count = 1;  // Number of retry
                while(true){
                    that_sock.send(request);
                    if(count < 1){
                        that_sock.close();
                        break;
                    }
                    count--;
                    try{
                        Thread.sleep(1000);
                    }
                    catch(Exception ex){
                        ex.printStackTrace();
                    }
                }

            }
            public void onClose(){
                System.out.println("* socket closed");
            }
        };
    }


    public void addEventHandler(TcpClientEventHandler handler){
        this.handler = handler;
    }
}

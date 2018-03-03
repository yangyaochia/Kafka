package com.scu.coen317;

import java.io.BufferedReader;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

//import javafx.util.Pair;
import java.io.*;

import java.util.ArrayList;
import java.util.List;
//import javafx.util.Pair;
import java.io.*;

public class Producer {
    String ip;
    int port;

    public Producer(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
    public void sendMessage() throws IOException {
        //String sentence;
        String modifiedSentence;
        Topic t1 = new Topic("Test1", 1,1);
        Topic t2 = new Topic("Test2", 1,1);
        List<Object> mylist = new ArrayList<>();
        mylist.add(t1);
        mylist.add(t2);
        //BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket = new Socket("localhost", 6789);
        ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //sentence = inFromUser.readLine();
        //outToServer.writeBytes(sentence + '\n');

        outToServer.writeObject(mylist);
        modifiedSentence = inFromServer.readLine();
        System.out.println("FROM SERVER: " + modifiedSentence);
        clientSocket.close();
    }
    /*public void sendMessage(Topic topic, String msg) throws IOException {
        String metaData;
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket = new Socket("localhost", 6789);
        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        sentence = inFromUser.readLine();
        outToServer.writeBytes(sentence + '\n');
        modifiedSentence = inFromServer.readLine();
        System.out.println("FROM SERVER: " + modifiedSentence);
        clientSocket.close();
    }
    /*public void sendMessage(Topic topic, String msg) throws IOException {
        String metaData;
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket = new Socket(ip, port);

        ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
        BufferedReader inFromServer =
                new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        //sentence = inFromUser.readLine();
        outToServer.writeObject(topic);
        outToServer.writeBytes(msg + '\n');
        metaData = inFromServer.readLine();
        System.out.println("FROM SERVER: " + metaData);

        clientSocket.close();
    }*/
    public static void main(String argv[]) throws Exception {
        Producer p = new Producer("localhost", 9000);
        p.sendMessage();
    }
}
package com.scu.coen317;

import java.io.*;
import java.net.*;

public class Broker {
    String ip;
    int port;
    public Broker(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
    public void receive_msg() throws IOException {
        String clientSentence;
        String capitalizedSentence;
        ServerSocket welcomeSocket = new ServerSocket(6789);
        while(true) {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

            DataOutputStream outToClient = new DataOutputStream (connectionSocket.getOutputStream());
            clientSentence = inFromClient.readLine();
            capitalizedSentence = clientSentence.toUpperCase() + '\n';
            outToClient.writeBytes (capitalizedSentence);
        }
    }

    public static void main(String argv[]) throws Exception {
        Broker b = new Broker("localhost", 6789);
        b.receive_msg();
    }
}


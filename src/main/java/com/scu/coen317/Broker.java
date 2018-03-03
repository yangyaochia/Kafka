package com.scu.coen317;

import java.io.*;
import java.net.*;

public class Broker {
    public static void main(String argv[]) throws Exception {
        String clientSentence;
        String response;
        ServerSocket welcomeSocket = new ServerSocket(9000);
        while (true) {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            clientSentence = inFromClient.readLine();
            response = "received : " + clientSentence;
            outToClient.writeBytes(response);
        }
    }
}


package com.scu.coen317;

public class Producer {
        String ip;
        String port;
    public:
        Producer(String ip, String port) {
            this.ip = ip;
            this.port = port;
        }
        void sendMessage(String topic, String message) {

            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
            Socket clientSocket = new Socket (ip, port);

            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer =
                    new BufferedReader(new InputStreamReader (clientSocket.getInputStream ()));

            outToServer.writeBytes(sentence + '\n');
            modifiedSentence = inFromServer.readLine();
            System.out.println("FROM SERVER: " + modifiedSentence);

            clientSocket.close();
        }
}
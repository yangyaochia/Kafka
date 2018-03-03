package com.scu.coen317;

public class Producer {
        string ip;
        string port;
    public:
        Producer(string ip, string port) {
            this.ip = ip;
            this.port = port;
        }
        void sendMessage() {
            String sentence;
            String modifiedSentence;
            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
            Socket clientSocket = new Socket (ip, port);

            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer =
                    new BufferedReader(new InputStreamReader (clientSocket.getInputStream ()));

            sentence = inFromUser.readLine();
            outToServer.writeBytes(sentence + '\n');
            modifiedSentence = inFromServer.readLine();
            System.out.println("FROM SERVER: " + modifiedSentence);

            clientSocket.close();
        }
}
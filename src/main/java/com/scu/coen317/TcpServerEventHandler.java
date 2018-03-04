package com.scu.coen317;

public interface TcpServerEventHandler extends Handler {
    public void onMessage(int client_id, String line);
    public void onAccept(int client_id);
    public void onClose(int client_id);
}

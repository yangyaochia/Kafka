package com.scu.coen317;

import java.util.List;

public interface TcpServerEventHandler {
    public void onMessage(int client_id, List<Object> msg);
    public void onAccept(int client_id);
    public void onClose(int client_id);
}

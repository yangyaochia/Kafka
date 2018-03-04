package com.scu.coen317;

public interface TcpClientEventHandler {
    void onMessage(String line);
    void onOpen();
    void onClose();
}

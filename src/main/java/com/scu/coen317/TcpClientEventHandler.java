package com.scu.coen317;

import java.util.List;

public interface TcpClientEventHandler {
    void onMessage(List<Object> message);
    void onOpen();
    void onClose();
}

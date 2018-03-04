package com.scu.coen317;

import java.util.List;

public interface TcpClientEventHandler extends Handler{
    void onMessage(List<Object> msg);
    void onOpen();
    void onClose();
}

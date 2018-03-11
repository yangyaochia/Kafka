package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public interface TcpServerEventHandler {
    public void onMessage(int client_id, Message message) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException;
    public void onAccept(int client_id);
    public void onClose(int client_id);
}

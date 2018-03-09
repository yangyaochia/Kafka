package com.scu.coen317;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public interface TcpClientEventHandler {
    void onMessage(Message massage) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, InterruptedException;
    void onOpen() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InterruptedException;
    void onClose();

}

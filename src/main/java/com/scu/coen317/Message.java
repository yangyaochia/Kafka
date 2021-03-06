package com.scu.coen317;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    MessageType methodName;
    List<Object> arguments;
    boolean isAck;

    public Message(MessageType methodName) {
        this.methodName = methodName;
        arguments = new ArrayList();
        this.isAck = false;
    }
    public Message(MessageType methodName, List<Object> arguments, boolean isAck) {
        this.methodName = methodName;
        this.arguments = arguments;
        this.isAck = isAck;
    }

    public Message(MessageType methodName, List<Object> arguments) {
        this.methodName = methodName;
        this.arguments = arguments;
    }
    public MessageType getMethodName() {
        return methodName;
    }
    public String getMethodNameValue() {
        return methodName.toString();
    }

    public void setIsAck(boolean isAck) {
        this.isAck = isAck;
    }
    public List<Object> getArguments() {
        return arguments;
    }

    public Class<?>[] getInputParameterType() {
        Class<?>[] inputs = new Class<?>[this.arguments.size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = this.getArguments().get(i).getClass();
        }
        return inputs;
    }

    public Object[] getInputValue() {
        Object[] inputs = new Object[this.arguments.size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = this.getArguments().get(i);
        }
        return inputs;
    }

    public boolean isAck() { return this.isAck; }
}
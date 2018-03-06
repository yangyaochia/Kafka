package com.scu.coen317;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;


public class Message implements Serializable {
    MessageType methodName;
    List<Object> arguments;

    public Message(MessageType methodName) {
        this.methodName = methodName;
        arguments = new ArrayList();
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
}
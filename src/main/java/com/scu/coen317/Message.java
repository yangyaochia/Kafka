package com.scu.coen317;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;


public class Message {
    String methodName;
    List<Object> arguments;

    public Message(String methodName) {
        this.methodName = methodName;
    }

    public Message(String methodName, List<Object> arguments) {
        this.methodName = methodName;
        this.arguments = arguments;
    }

    public String getMethodName() {
        return methodName;
    }

    public List<Object> getArguments() {
        return arguments;
    }

//    public Class<?> getParameterTypes() {
//        List<Object> types = new ArrayList();
//        for (Object o : arguments) {
//            types.add(o.getClass());
//        }
//        return types;
//    }

    public void find(String t, Integer i) {
        System.out.println("Success");
    }

    public Class<?>[] toArray() {
        Class<?>[] inputs = new Class<?>[this.arguments.size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = this.getArguments().get(i).getClass();
        }
        return inputs;
    }

    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Message message = new Message("find");
        message.arguments.add("most useful");
        message.arguments.add(1);
        //message.find("test", 1);

        Class<?>[] inputTypes = message.toArray();
        System.out.println(message.getMethodName());
//        System.out.println(message.getParameterTypes().toString());
        Class clazz = Message.class;
        Method method = clazz.getMethod(message.methodName, inputTypes);
        Object[] inputs = new Object[message.arguments.size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = message.getArguments().get(i);
        }
        method.invoke(message, inputs);
        Method[] methods = clazz.getDeclaredMethods();
        for (Method m : methods) {
            System.out.println("m's name : " + m.getName());
            Type[] pType = m.getGenericParameterTypes();
//            if (m.getName().equals(message.getMethodName())) {
//                Object[] inputs = new Object[message.arguments.size()];
//                for (int i = 0; i < inputs.length; i++) {
//                    inputs[i] = message.getArguments().get(i);
//                }
//                m.invoke(message, inputs);
            break;
        }

    }

}

package com.scu.coen317;

public class HostRecord {
    String host;
    String port;
    HostRecord(String host,String port) {
        this.host = host;
        this.port = port;
    }
    String getHost() { return host;}
    String getPort() { return port;}
}

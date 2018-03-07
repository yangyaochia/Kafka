package com.scu.coen317;

import java.io.Serializable;

public class HostRecord implements Serializable {
    String host;
    Integer port;
    HostRecord(String host, Integer port) {
        this.host = host;
        this.port = port;
    }
    String getHost() { return host;}
    Integer getPort() { return port;}
}

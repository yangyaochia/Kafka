package com.scu.coen317;

import java.io.Serializable;

public class HostRecord implements Serializable {
    private String host;
    private Integer port;
    public HostRecord(String host, Integer port) {
        this.host = host;
        this.port = port;
    }
    
    String getHost() { return host;}
    Integer getPort() { return port;}

    @Override
    public boolean equals(Object that) {
        return ((HostRecord)that) != null
                && that instanceof HostRecord
                && ((HostRecord)that).host.equals(host)
                && ((HostRecord)that).getPort().equals(port);
    }

    @Override 
    public int hashCode() {
        int hash = 5381;
        int i = 0;
        while (i < host.length()) {
            hash = ((hash << 5) + hash) + host.charAt(i++); /* hash * 33 + c */
        }
        return hash * 31 + port;
    }

    @Override
    public String toString() {
        return "hostName: " + this.host + ", portNumber: " + this.port;
    }
}

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

    @Override
    public boolean equals(Object that) {
        return ((HostRecord)that) != null && that instanceof HostRecord && ((HostRecord)that).host == host && ((HostRecord)that).port == port;
    }

    @Override public int hashCode() {
        int hash = 5381;
        int i = 0;
        while (i < host.length()) {
            hash = ((hash << 5) + hash) + host.charAt(i++); /* hash * 33 + c */
        }
        return hash * 31 + port;
    }
}

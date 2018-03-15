package com.scu.coen317;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class DataCache<K, V> {
    private final long mDefaultTimeout = 15;
    private long mTimeout = 0;
    private HashMap<K, DataValue<V>> dataMap;

    DataCache() {
        dataMap = new HashMap<K, DataValue<V>>();
        mTimeout = mDefaultTimeout;
    }

    DataCache(long timeoutMinutes) {
        dataMap = new HashMap<K, DataValue<V>>();
        mTimeout = timeoutMinutes;
    }

    public void put(K key, V value) {
        dataMap.put(key, new DataValue<V>(value));
    }

    public V get(K key) {
        DataValue<V> data = dataMap.get(key);
        V result = null;
        if (data != null) {
            long diff = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - data.insertTime);
            if (diff >= mTimeout) {
                dataMap.remove(key);
                data.value = null;
            }
            result = data.value;
        }
        return result;
    }

    public boolean containsKey(K key) {
        if (dataMap.containsKey(key)) {
            this.get(key);
        }
        return dataMap.containsKey(key);
    }

    public void setTimeout(long minutes) {
        mTimeout = minutes;
    }

    public long getTimeout() {
        return mDefaultTimeout;
    }

    public void remove(K key) {
        if (dataMap.containsKey(key)) {
            dataMap.remove(key);
        }
    }

    private final class DataValue<T> {
        public T value;
        public long insertTime;

        DataValue(T value) {
            this.value = value;
            insertTime = System.currentTimeMillis();
        }
    }
}

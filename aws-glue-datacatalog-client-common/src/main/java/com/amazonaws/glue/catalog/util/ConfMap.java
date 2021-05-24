package com.amazonaws.glue.catalog.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConfMap implements Map<String, String> {
    Map<String, String> delegate;

    private String unsupportedOperationMsg = "ConfMap is read-only.";

    public ConfMap(Map<String, String> original) {
        this.delegate = original;
    }

    public ConfMap() {
        this.delegate = new HashMap<>();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return delegate.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return delegate.containsValue(o);
    }

    @Override
    public String get(Object o) {
        return delegate.get(o);
    }

    @Override
    public String put(String key, String value) {
        throw new UnsupportedOperationException(unsupportedOperationMsg);
    }

    @Override
    public String remove(Object key) {
        throw new UnsupportedOperationException(unsupportedOperationMsg);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        throw new UnsupportedOperationException(unsupportedOperationMsg);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(unsupportedOperationMsg);
    }


    @Override
    public Set<String> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<String> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return delegate.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    /**
     * Returns the boolean value to which the specified key is mapped,
     * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key);
        // We can't use `Boolean.parseBoolean` here, as it returns false for invalid strings.
        if (value == null) {
            return defaultValue;
        } else if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new IllegalArgumentException(value + " is not a boolean string.");
        }
    }

    /**
     * Returns the integer value to which the specified key is mapped,
     * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
     */
    public int getInt(String key, int defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    /**
     * Returns the long value to which the specified key is mapped,
     * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
     */
    public long getLong(String key, long defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    /**
     * Returns the double value to which the specified key is mapped,
     * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
     */
    public double getDouble(String key, double defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Double.parseDouble(value);
    }

    public Class<?> getClass(String name, Class<?> defaultValue) {
        if (get(name) == null) {
            return defaultValue;
        } else {
            try {
                return Class.forName(get(name));
            } catch (ClassNotFoundException var5) {
                throw new RuntimeException(var5);
            }
        }
    }
}

package com.amazonaws.glue.catalog.converters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkewedValueConverter {

    public static String convertListToString(final List<String> list) {
        if (list == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            String currentString = list.get(i);
            sb.append(currentString.length() + "$" + currentString);
        }

        return sb.toString();
    }

    public static Map<String, String> convertSkewedMap(final Map<List<String>, String> coreSkewedMap){
        if (coreSkewedMap == null){
            return null;
        }
        Map<String, String> catalogSkewedMap = new HashMap<>();
        for (List<String> coreKey : coreSkewedMap.keySet()) {
            catalogSkewedMap.put(convertListToString(coreKey), coreSkewedMap.get(coreKey));
        }
        return catalogSkewedMap;
    }

    public static List<String> convertSkewedValue(final List<List<String>> coreSkewedValue) {
        if (coreSkewedValue == null) {
            return null;
        }
        List<String> catalogSkewedValue = new ArrayList<>();
        for (int i = 0; i < coreSkewedValue.size(); i++) {
            catalogSkewedValue.add(convertListToString(coreSkewedValue.get(i)));
        }

        return catalogSkewedValue;
    }
}

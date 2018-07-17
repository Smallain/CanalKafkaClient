package com.smallain.util;

import java.lang.reflect.Array;
import java.util.*;

public class ArgsParse {
    public static Map<String, String> getArgsMap(ArrayList parameters) {
        List keyArray = new ArrayList();
        List valueArray = new ArrayList();

        for (int i = 0; i < parameters.size(); i++) {
            //int indexStr = parameters.indexOf(i);
            if (i % 2 == 0) {
                keyArray.add(parameters.get(i));
            } else if (i % 2 == 1) {
                valueArray.add(parameters.get(i));
            }
        }

        Map kvMap = new HashMap();
        for (int j = 0; j < keyArray.size(); j++) {
            String keys = keyArray.get(j).toString();
            String values = valueArray.get(j).toString();
            kvMap.put(keys, values);
        }
        return kvMap;
    }

}
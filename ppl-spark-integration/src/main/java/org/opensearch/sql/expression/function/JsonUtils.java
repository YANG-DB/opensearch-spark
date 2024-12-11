/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface JsonUtils {
    ObjectMapper objectMapper = new ObjectMapper();
    
    static Object parseValue(String value) {
        // Try parsing the value as JSON, fallback to primitive if parsing fails
        try {
            return objectMapper.readValue(value, Object.class);
        } catch (Exception e) {
            // Primitive value, return as is
            return value;
        }
    }
    
    /**
     * append nested value to the json object
     * @param currentObj
     * @param pathParts
     * @param depth
     * @param valueToAppend
     */
    static void appendNestedValue(Object currentObj, String[] pathParts, int depth, Object valueToAppend) {
        if (currentObj == null || depth >= pathParts.length) {
            return;
        }

        if (currentObj instanceof Map) {
            Map<String, Object> currentMap = (Map<String, Object>) currentObj;
            String currentKey = pathParts[depth];

            if (depth == pathParts.length - 1) {
                // If it's the last key, append to the array
                currentMap.computeIfAbsent(currentKey, k -> new ArrayList<>()); // Create list if not present
                Object existingValue = currentMap.get(currentKey);

                if (existingValue instanceof List) {
                    List<Object> existingList = (List<Object>) existingValue;
                    existingList.add(valueToAppend);
                }
            } else {
                // Continue traversing
                currentMap.computeIfAbsent(currentKey, k -> new LinkedHashMap<>()); // Create map if not present
                appendNestedValue(currentMap.get(currentKey), pathParts, depth + 1, valueToAppend);
            }
        } else if (currentObj instanceof List) {
            // If the current object is a list, process each map in the list
            List<Object> list = (List<Object>) currentObj;
            for (Object item : list) {
                if (item instanceof Map) {
                    appendNestedValue(item, pathParts, depth, valueToAppend);
                }
            }
        }
    }

    /**
     * remove nested json object using its keys parts
     * @param currentObj
     * @param keyParts
     * @param depth
     */
    static void removeNestedKey(Object currentObj, String[] keyParts, int depth) {
        if (currentObj == null || depth >= keyParts.length) {
            return;
        }

        if (currentObj instanceof Map) {
            Map<String, Object> currentMap = (Map<String, Object>) currentObj;
            String currentKey = keyParts[depth];

            if (depth == keyParts.length - 1) {
                // If it's the last key, remove it from the map
                currentMap.remove(currentKey);
            } else {
                // If not the last key, continue traversing
                if (currentMap.containsKey(currentKey)) {
                    Object nextObj = currentMap.get(currentKey);

                    if (nextObj instanceof List) {
                        // If the value is a list, process each item in the list
                        List<Object> list = (List<Object>) nextObj;
                        for (int i = 0; i < list.size(); i++) {
                            removeNestedKey(list.get(i), keyParts, depth + 1);
                        }
                    } else {
                        // Continue traversing if it's a map
                        removeNestedKey(nextObj, keyParts, depth + 1);
                    }
                }
            }
        }
    }
}

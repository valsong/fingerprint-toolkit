package com.valsong.fingerprint.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.valsong.fingerprint.StringUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static com.valsong.fingerprint.ElasticToolConstants.ERROR;
import static com.valsong.fingerprint.ElasticToolConstants.NONE;


/**
 * JsonToolkit
 *
 * @author Val Song
 */
public class JsonToolkit {

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        ObjectMapper objectMapper = new ObjectMapper();
        // NOTE 为了保证生成指纹可靠性，不要设置JsonInclude.Include.NON_NULL
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
        OBJECT_MAPPER = objectMapper;
    }

    /**
     * 获取指纹ID,如果抛出异常了，则指纹ID为ERROR，如果json为空则返回NONE
     *
     * @param json json
     * @return 指纹ID
     */
    public static String fingerprintId(String json) {
        if (StringUtils.isBlank(json)) {
            return NONE;
        }
        try {
            String jsonKeys = jsonKeys(json);
            if (ERROR.equals(jsonKeys) || NONE.equals(jsonKeys)) {
                return jsonKeys;
            }
            // sha256
            return DigestUtils.sha256Hex(jsonKeys);
        } catch (Throwable e) {
            return ERROR;
        }
    }


    /**
     * 获取json中所有的key并进行排序
     *
     * @param json json
     * @return 排完序的json keys
     */
    public static String jsonKeys(String json) {
        if (StringUtils.isBlank(json)) {
            return NONE;
        }
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readValue(json, JsonNode.class);
            // NOTE 这一步不能少，为了去除数组中的重复元素
            jsonNode = JsonToolkit.setUpDefaultValue(jsonNode);
            return JsonToolkit.jsonKeys(jsonNode);
        } catch (Throwable e) {
            return ERROR;
        }
    }


    /**
     * 获取指纹,如果抛出异常了，则指纹为ERROR，如果json为空则返回NONE
     *
     * @param json json
     * @return 指纹
     */
    public static String fingerprint(String json) {
        if (StringUtils.isBlank(json)) {
            return NONE;
        }
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readValue(json, JsonNode.class);
            // NOTE 这一步不能少，为了去除数组中的重复元素
            jsonNode = JsonToolkit.setUpDefaultValue(jsonNode);
            return OBJECT_MAPPER.writeValueAsString(jsonNode);
        } catch (Throwable e) {
            return ERROR;
        }
    }


    /**
     * 获取json中所有的key并进行排序
     *
     * @param jsonObject jsonObject
     * @return 排完序的json keys
     */
    public static String jsonKeys(JsonNode jsonObject) {
        if (jsonObject == null) {
            return null;
        }
        List<String> keys = new ArrayList<>();
        Stack<Entry> stack = new Stack<>();
        stack.push(new Entry("", jsonObject));
        while (!stack.isEmpty()) {
            Entry entry = stack.pop();
            String prefix = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof ObjectNode) {
                ObjectNode currentJsonObject = (ObjectNode) value;
                if (currentJsonObject.isEmpty()) {
                    if (prefix.length() >= 1) {
                        keys.add(prefix.substring(0, prefix.length() - 1));
                    }
                } else {
                    Iterator<Map.Entry<String, JsonNode>> fields = currentJsonObject.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> subEntry = fields.next();
                        stack.push(new Entry(prefix + subEntry.getKey() + ".", subEntry.getValue()));
                    }
                }
            } else if (value instanceof ArrayNode) {
                ArrayNode currentJsonArray = (ArrayNode) value;
                for (int i = 0; i < currentJsonArray.size(); i++) {
                    stack.push(new Entry(prefix + i + ".", currentJsonArray.get(i)));
                }
            } else {
                keys.add(prefix.substring(0, prefix.length() - 1));
            }
        }
        Collections.sort(keys);
        return String.join(",", keys);
    }


    /**
     * 将json中所有的value值设置为?,如果遇到数组则对相同的值进行合并
     *
     * @param jsonObject
     * @return json中所有的value值设置为?
     */
    public static JsonNode setUpDefaultValue(JsonNode jsonObject) {
        return doSetUpDefaultValue(jsonObject);
    }

    /**
     * 将json中所有的value值设置为?,如果遇到数组则对相同的值进行合并
     *
     * @param value 可能为jsonObject、jsonArray、其他类型
     * @return json中所有的value值设置为?
     */
    public static JsonNode doSetUpDefaultValue(JsonNode value) {
        if (value instanceof ObjectNode) {
            ObjectNode jsonObject = (ObjectNode) value;

            Iterator<Map.Entry<String, JsonNode>> fields = jsonObject.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                JsonNode entryValue = entry.getValue();
                entry.setValue(doSetUpDefaultValue(entryValue));
            }
        } else if (value instanceof ArrayNode) {
            ArrayNode jsonArray = (ArrayNode) value;
            for (int i = 0; i < jsonArray.size(); i++) {
                JsonNode currentVal = jsonArray.get(i);
                jsonArray.set(i, doSetUpDefaultValue(currentVal));
            }
            // NOTE 针对多个值进行去重
            Set<JsonNode> unionObjSet = new HashSet<>();
            Iterator<JsonNode> iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                unionObjSet.add(iterator.next());
            }

            jsonArray.removeAll();
            jsonArray.addAll(unionObjSet);
        } else {
            value = new TextNode("?");
        }
        return value;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Entry {

        private String key;

        private Object value;

    }

    /**
     * 将java对象序列化为json
     *
     * @param object object
     * @param <T>
     * @return json
     */
    public static <T> String toJson(T object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * 将json反序列化为java对象
     *
     * @param json  json
     * @param clazz java对象类型
     * @param <T>
     * @return java对象
     */
    public static <T> T parse(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("json:[%s] %s", json, e.getMessage()), e);
        }
    }

}

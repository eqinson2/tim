package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import com.ericsson.ema.tim.zookeeper.ZKPersistenceUtil;
import org.apache.zookeeper.KeeperException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AbstractChangeOperator extends AbstractOperator implements Operator {
    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractChangeOperator.class);

    List<Object> cloneList(List<Object> original) {
        if (original == null || original.size() == 0) {
            return new ArrayList<>();
        }

        try {
            Method cloneMethod = original.get(0).getClass().getDeclaredMethod("clone");
            cloneMethod.setAccessible(true);
            List<Object> clonedList = new ArrayList<>();

            for (Object anOriginal : original) {
                // noinspection unchecked
                clonedList.add(cloneMethod.invoke(anOriginal));
            }
            return clonedList;
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            LOGGER.error("Couldn't clone list due to " + e.getMessage());
            return Collections.emptyList();
        }
    }

    void persist(List<Object> listOfObj) throws KeeperException.ConnectionLossException {
        JSONObject json = new JSONObject();
        JSONObject tableBody = new JSONObject();
        json.put("Table", tableBody);

        tableBody.put("Id", this.table);

        JSONArray headerArray = new JSONArray();
        tableBody.put("Header", headerArray);

        getContext().getTableMetadata().forEach((k, v) -> {
            JSONObject item = new JSONObject();
            item.put(k, v);
            headerArray.put(item);
        });

        JSONArray contentArray = new JSONArray();
        tableBody.put("Content", contentArray);

        for (Object obj : listOfObj) {
            JSONObject item = new JSONObject();
            item.put("Tuple", obj.toString());
            contentArray.put(item);
        }

        ZKPersistenceUtil zku = new ZKPersistenceUtil();
        zku.persist(this.table, json.toString());
    }

    Object realValue(String[] updateVal) {
        String field = updateVal[0];
        String newValue = updateVal[1];
        Map<String, String> metadata = getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        if (fieldType == null)
            throw new DmlNoSuchFieldException(field);

        Object newObject;
        switch (fieldType) {
            case DataTypes.String:
                newObject = newValue;
                break;
            case DataTypes.Int:
                newObject = Integer.valueOf(newValue);
                break;
            case DataTypes.Boolean:
                newObject = Boolean.valueOf(newValue);
                break;
            default:
                throw new DmlBadSyntaxException("unsupported data type: " + field + "," + fieldType);
        }
        return newObject;
    }
}

package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.reflection.MethodInvocationCache;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static com.ericsson.ema.tim.dml.TableInfoMap.tableInfoMap;
import static com.ericsson.ema.tim.reflection.MethodInvocationCache.AccessType.GET;
import static com.ericsson.ema.tim.reflection.MethodInvocationCache.AccessType.SET;
import static com.ericsson.ema.tim.reflection.Tab2MethodInvocationCacheMap.tab2MethodInvocationCacheMap;

public class AbstractOperator implements Operator {
    private final static String TUPLE_FIELD = "records";
    protected String table;
    List<Object> records;
    private TableInfoContext context;
    private MethodInvocationCache methodInvocationCache;

    @Override
    public TableInfoContext getContext() {
        return context;
    }

    @Override
    public MethodInvocationCache getMethodInvocationCache() {
        return methodInvocationCache;
    }

    void initExecuteContext() {
        this.context = tableInfoMap.lookup(table).orElseThrow(() -> new DmlBadSyntaxException("Error: Updating a " +
                "non-existing table:" + table));
        this.methodInvocationCache = tab2MethodInvocationCacheMap.lookup(table);

        //it is safe because records must be List according to JavaBean definition
        Object tupleField = invokeGetByReflection(context.getTabledata(), TUPLE_FIELD);
        assert (tupleField instanceof List<?>);
        //noinspection unchecked
        this.records = (List<Object>) tupleField;
    }

    Object invokeGetByReflection(Object obj, String wantedField) {
        Method getter = methodInvocationCache.get(obj.getClass(), wantedField, GET);
        try {
            return getter.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DmlBadSyntaxException(e.getMessage());//should never happen
        }
    }

    void invokeSetByReflection(Object obj, String wantedField, Object newValue) {
        Method setter = methodInvocationCache.get(obj.getClass(), wantedField, SET);
        try {
            setter.invoke(obj, newValue);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DmlBadSyntaxException(e.getMessage());//should never happen
        }
    }
}

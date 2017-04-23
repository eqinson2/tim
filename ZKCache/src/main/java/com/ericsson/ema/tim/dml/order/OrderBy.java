package com.ericsson.ema.tim.dml.order;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.dml.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ericsson.ema.tim.reflection.MethodInvocationCache.AccessType.GET;

/**
 * Created by eqinson on 2017/4/23.
 */
public class OrderBy {
    private final static Logger LOGGER = LoggerFactory.getLogger(OrderBy.class);

    private final String field;
    private final boolean reversed;

    private Select selector;

    private OrderBy(String field, boolean reversed) {
        this.field = field;
        this.reversed = reversed;
    }

    public static OrderBy orderby(String field, String asc) {
        if (asc.toUpperCase().equals("DESC"))
            return new OrderBy(field, true);
        else if (asc.toUpperCase().equals("ASC"))
            return new OrderBy(field, false);
        else throw new IllegalArgumentException("Error: orderby must be either asc or desc");
    }

    public static OrderBy orderby(String field) {
        return new OrderBy(field, false);
    }

    public void setSelector(Select selector) {
        this.selector = selector;
    }

    public String getField() {
        return this.field;
    }

    public Comparator<Object> comparing() {
        if (!selector.getSelectedFields().isEmpty() && !selector.getSelectedFields().contains(field))
            throw new IllegalArgumentException(String.format("Error: order by parameter %s not in selected field %s",
                field, selector.getSelectedFields().stream().collect(Collectors.joining(",", "{", "}"))));

        Map<String, String> metadata = selector.getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        switch (fieldType) {
            case DataTypes.String:
                return (o1, o2) -> {
                    String s1 = (String) getFiledValFromTupleByName(o1);
                    String s2 = (String) getFiledValFromTupleByName(o2);
                    return !reversed ? s1.compareTo(s2) : s2.compareTo(s1);
                };
            case DataTypes.Int:
                return (o1, o2) -> {
                    Integer s1 = (Integer) getFiledValFromTupleByName(o1);
                    Integer s2 = (Integer) getFiledValFromTupleByName(o2);
                    return !reversed ? s1.compareTo(s2) : s2.compareTo(s1);
                };
            default:
                LOGGER.error("unsupported data type: {}", fieldType);
                throw new RuntimeException("unsupported data type: " + fieldType);
        }
    }

    private Object getFiledValFromTupleByName(Object tuple) {
        Method getter = selector.getMethodInvocationCache().get(tuple.getClass(), field, GET);
        try {
            return getter.invoke(tuple);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());//should never happen
        }
    }
}

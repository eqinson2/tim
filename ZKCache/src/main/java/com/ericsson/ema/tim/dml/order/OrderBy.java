package com.ericsson.ema.tim.dml.order;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.dml.SelectClause;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by eqinson on 2017/4/23.
 */
public class OrderBy extends SelectClause {
    private final static Logger LOGGER = LoggerFactory.getLogger(OrderBy.class);

    private final String field;
    private final boolean reversed;

    private OrderBy(String field, boolean reversed) {
        this.field = field;
        this.reversed = reversed;
    }

    public static OrderBy orderby(String field, String asc) {
        if (asc.toUpperCase().equals("DESC"))
            return new OrderBy(field, true);
        else if (asc.toUpperCase().equals("ASC"))
            return new OrderBy(field, false);
        else throw new DmlBadSyntaxException("Error: orderBy must be either asc or desc");
    }

    public static OrderBy orderby(String field) {
        return new OrderBy(field, false);
    }

    public String getField() {
        return this.field;
    }

    public Comparator<Object> comparing() {
        if (!getSelector().getSelectedFields().isEmpty() && !getSelector().getSelectedFields().contains(field))
            throw new IllegalArgumentException(String.format("Error: order by parameter %s not in selected field %s",
                field, getSelector().getSelectedFields().stream().collect(Collectors.joining(",", "{", "}"))));

        Map<String, String> metadata = getSelector().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        if (fieldType == null)
            throw new DmlNoSuchFieldException(field);

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
            case DataTypes.Boolean:
                return (o1, o2) -> {
                    Boolean b1 = (Boolean) getFiledValFromTupleByName(o1);
                    Boolean b2 = (Boolean) getFiledValFromTupleByName(o2);
                    return !reversed ? b1.compareTo(b2) : b2.compareTo(b1);
                };
            default:
                LOGGER.error("unsupported data type: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("Error: unsupported data type: " + field + "," + fieldType);
        }
    }
}

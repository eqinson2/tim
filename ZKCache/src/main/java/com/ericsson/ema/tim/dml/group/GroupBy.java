package com.ericsson.ema.tim.dml.group;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import com.ericsson.ema.tim.dml.SelectClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by eqinson on 2017/4/23.
 */
public class GroupBy extends SelectClause {
    private final static Logger LOGGER = LoggerFactory.getLogger(GroupBy.class);

    private final String field;

    private GroupBy(String field) {
        this.field = field;
    }

    public static GroupBy groupBy(String field) {
        return new GroupBy(field);
    }

    public String getField() {
        return this.field;
    }

    public Function<Object, Object> grouping() {
        Map<String, String> metadata = getSelector().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        if (fieldType == null)
            throw new DmlNoSuchFieldException(field);

        switch (fieldType) {
            case DataTypes.String:
            case DataTypes.Int:
                return this::getFiledValFromTupleByName;
            default:
                LOGGER.error("unsupported data type: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("Error: unsupported data type: " + field + "," + fieldType);
        }
    }
}

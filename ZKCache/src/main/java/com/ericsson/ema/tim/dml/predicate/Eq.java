package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Eq extends AbstractPredicate implements Predicate {
    private final static Logger LOGGER = LoggerFactory.getLogger(Eq.class);

    private Eq(String field, String value) {
        super(field, value);
    }

    public static Eq eq(String field, String value) {
        return new Eq(field, value);
    }

    @Override
    public boolean eval(Object tuple) {
        if (this.valueToComp == null)
            return false;

        Object fieldVal = getFiledValFromTupleByName(tuple);
        Map<String, String> metadata = getOperator().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        if (fieldType == null)
            throw new DmlNoSuchFieldException(field);

        switch (fieldType) {
            case DataTypes.String:
                return (this.valueToComp).equals(fieldVal);
            case DataTypes.Int:
                return (Integer.valueOf((String) this.valueToComp)).equals(fieldVal);
            case DataTypes.Boolean:
                return (Boolean.valueOf((String) this.valueToComp)).equals(fieldVal);
            default:
                LOGGER.error("unsupported data type: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("unsupported data type: " + field + "," + fieldType);
        }
    }
}

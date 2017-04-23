package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.dml.DmlBadSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by eqinson on 2017/4/23.
 */
public class UnEq extends AbstractPredicate implements Predicate {
    private final static Logger LOGGER = LoggerFactory.getLogger(UnEq.class);

    private UnEq(String field, String value) {
        super(field, value);
    }

    public static UnEq uneq(String field, String value) {
        return new UnEq(field, value);
    }

    @Override
    public boolean eval(Object tuple) {
        if (this.valueToComp == null)
            return true;

        Object fieldVal = getFiledValFromTupleByName(tuple);
        Map<String, String> metadata = getSelector().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        switch (fieldType) {
            case DataTypes.String:
                return !(this.valueToComp).equals(fieldVal);
            case DataTypes.Int:
                return !(Integer.valueOf((String) this.valueToComp)).equals(fieldVal);
            default:
                LOGGER.error("unsupported data type: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("unsupported data type: " + field + "," + fieldType);
        }
    }
}

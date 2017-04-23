package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by eqinson on 2017/4/23.
 */
public class LessThan extends AbstractPredicate implements Predicate {
    private final static Logger LOGGER = LoggerFactory.getLogger(LessThan.class);

    private LessThan(String field, int value) {
        super(field, value);
    }

    public static LessThan lt(String field, int value) {
        return new LessThan(field, value);
    }

    @Override
    public boolean eval(Object tuple) {
        if (this.valueToComp == null)
            return false;

        Object fieldVal = getFiledValFromTupleByName(tuple);
        Map<String, String> metadata = getSelector().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        if (fieldType == null)
            throw new DmlNoSuchFieldException(field);

        switch (fieldType) {
            case DataTypes.Int:
                return Integer.compare((Integer) fieldVal, (Integer) this.valueToComp) < 0;
            default:
                LOGGER.error("must be int type in LessThan: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("must be int type in LessThan: " + field + "," + fieldType);
        }
    }
}
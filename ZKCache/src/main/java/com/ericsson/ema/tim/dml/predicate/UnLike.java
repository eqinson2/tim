package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by eqinson on 2017/4/23.
 */
public class UnLike extends AbstractPredicate implements Predicate {
    private final static Logger LOGGER = LoggerFactory.getLogger(UnLike.class);

    private UnLike(String field, String value) {
        super(field, value);
    }

    public static UnLike unlike(String field, String value) {
        return new UnLike(field, value);
    }

    @Override
    public boolean eval(Object tuple) {
        if (this.valueToComp == null)
            return false;

        Object fieldVal = getFiledValFromTupleByName(tuple);
        Map<String, String> metadata = getSelector().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        switch (fieldType) {
            case DataTypes.String:
                return !((String) fieldVal).contains((String) this.valueToComp);
            default:
                LOGGER.error("unsupported data type: {},{}", field, fieldType);
                throw new RuntimeException("unsupported data type: " + field + "," + fieldType);
        }
    }
}

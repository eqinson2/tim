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
public class Range extends AbstractPredicate implements Predicate {
    private final static Logger LOGGER = LoggerFactory.getLogger(Range.class);
    private int from = Integer.MIN_VALUE;
    private int to = Integer.MAX_VALUE;

    private Range(String field, int from, int to) {
        super(field, null);
        if (from > to)
            throw new DmlBadSyntaxException("from must be lte to");
        this.from = from;
        this.to = to;
    }

    public static Range range(String field, int from, int to) {
        return new Range(field, from, to);
    }

    @Override
    public boolean eval(Object tuple) {
        Object fieldVal = getFiledValFromTupleByName(tuple);
        Map<String, String> metadata = getSelector().getContext().getTableMetadata();
        String fieldType = metadata.get(field);
        if (fieldType == null)
            throw new DmlNoSuchFieldException(field);

        switch (fieldType) {
            case DataTypes.Int:
                return Integer.compare((Integer) fieldVal, from) >= 0 && Integer.compare((Integer) fieldVal, to) < 0;
            default:
                LOGGER.error("must be int type in BiggerThan: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("must be int type in BiggerThan: " + field + "," + fieldType);
        }
    }
}

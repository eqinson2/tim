package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.DataTypes;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Like extends AbstractPredicate implements Predicate {
    private final static Logger LOGGER = LoggerFactory.getLogger(Eq.class);

    private Like(String field, String value) {
        super(field, value);
    }

    public static Like like(String field, String value) {
        return new Like(field, value);
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
            case DataTypes.String:
                Pattern p = Pattern.compile((String) this.valueToComp);
                Matcher m = p.matcher((String) fieldVal);
                return m.find();
            default:
                LOGGER.error("unsupported data type: {},{}", field, fieldType);
                throw new DmlBadSyntaxException("unsupported data type: " + field + "," + fieldType);
        }
    }
}
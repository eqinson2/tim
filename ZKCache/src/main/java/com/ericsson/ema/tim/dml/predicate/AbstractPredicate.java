package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.SelectClause;

public abstract class AbstractPredicate extends SelectClause {
    final String field;
    final Object valueToComp;

    AbstractPredicate(String field, Object value) {
        this.field = field;
        this.valueToComp = value;
    }

    @Override
    public String getField() {
        return field;
    }

    abstract public boolean eval(Object tuple);
}

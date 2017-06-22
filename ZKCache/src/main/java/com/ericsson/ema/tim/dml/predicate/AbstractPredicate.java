package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.Clause;

public abstract class AbstractPredicate extends Clause {
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

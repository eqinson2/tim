package com.ericsson.ema.tim.dml.predicate;

import com.ericsson.ema.tim.dml.Select;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.ericsson.ema.tim.reflection.MethodInvocationCache.AccessType.GET;

public abstract class AbstractPredicate {
    final String field;
    final Object valueToComp;

    private Select selector;

    AbstractPredicate(String field, Object value) {
        this.field = field;
        this.valueToComp = value;
    }

    Select getSelector() {
        return selector;
    }

    public void setSelector(Select selector) {
        this.selector = selector;
    }

    Object getFiledValFromTupleByName(Object tuple) {
        Method getter = getSelector().getMethodInvocationCache().get(tuple.getClass(), field, GET);
        try {
            return getter.invoke(tuple);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());//should never happen
        }
    }

    abstract public boolean eval(Object tuple);
}

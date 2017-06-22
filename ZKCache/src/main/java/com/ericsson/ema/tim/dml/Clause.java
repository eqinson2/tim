package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.exception.DmlBadSyntaxException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.ericsson.ema.tim.reflection.MethodInvocationCache.AccessType.GET;

/**
 * Created by eqinson on 2017/4/23.
 */
//public abstract class SelectClause {
//    private Select selector;
//
//    protected Select getSelector() {
//        return selector;
//    }
//
//    public void setSelector(Select selector) {
//        this.selector = selector;
//    }
//
//    public abstract String getField();
//
//    protected Object getFiledValFromTupleByName(Object tuple) {
//        Method getter = getSelector().getMethodInvocationCache().get(tuple.getClass(), getField(), GET);
//        try {
//            return getter.invoke(tuple);
//        } catch (IllegalAccessException | InvocationTargetException e) {
//            throw new DmlBadSyntaxException(e.getMessage());//should never happen
//        }
//    }
//}

public abstract class Clause {
    private Operator operator;

    protected Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public abstract String getField();

    protected Object getFiledValFromTupleByName(Object tuple) {
        Method getter = getOperator().getMethodInvocationCache().get(tuple.getClass(), getField(), GET);
        try {
            return getter.invoke(tuple);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DmlBadSyntaxException(e.getMessage());//should never happen
        }
    }
}

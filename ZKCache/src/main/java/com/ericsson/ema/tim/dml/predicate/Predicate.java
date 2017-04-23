package com.ericsson.ema.tim.dml.predicate;

/**
 * Created by eqinson on 2017/4/23.
 */
public interface Predicate {
    boolean eval(Object tuple);
}

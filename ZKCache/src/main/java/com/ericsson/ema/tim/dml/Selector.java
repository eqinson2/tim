package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.dml.predicate.Predicate;

import java.util.List;

/**
 * Created by eqinson on 2017/4/18.
 */
public interface Selector {
    Selector from(String tab);

    Selector where(Predicate predicate);

    Selector orderby(String field, String asc);

    Selector orderby(String field);

    Selector limit(int limit);

    Selector skip(int skip);

    List<Object> execute();

    List<List<Object>> executeWithSelectFields();
}

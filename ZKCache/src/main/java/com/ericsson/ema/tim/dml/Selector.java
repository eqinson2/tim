package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.dml.predicate.Predicate;

import java.util.List;
import java.util.Map;

/**
 * Created by eqinson on 2017/4/18.
 */
public interface Selector {
    Selector from(String tab);

    Selector where(Predicate predicate);

    Selector orderBy(String field, String asc);

    Selector orderBy(String field);

    Selector groupBy(String field);

    Selector limit(int limit);

    Selector skip(int skip);

    List<Object> collect();

    List<List<Object>> collectBySelectFields();

    Map<Object, List<Object>> collectByGroup();

    long count();

    boolean exists();
}

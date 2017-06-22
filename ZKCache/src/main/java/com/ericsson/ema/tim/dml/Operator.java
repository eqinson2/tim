package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.reflection.MethodInvocationCache;

/**
 * Created by eqinson on 2017/6/21.
 */
public interface Operator {
    MethodInvocationCache getMethodInvocationCache();

    TableInfoContext getContext();
}

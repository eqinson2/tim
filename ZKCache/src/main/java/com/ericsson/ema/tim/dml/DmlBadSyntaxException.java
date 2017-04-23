package com.ericsson.ema.tim.dml;

/**
 * Created by eqinson on 2017/4/23.
 */
public class DmlBadSyntaxException extends RuntimeException {
    public DmlBadSyntaxException(String error) {
        super(error);
    }
}

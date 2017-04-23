package com.ericsson.ema.tim.exception;

/**
 * Created by eqinson on 2017/4/23.
 */
public class DmlNoSuchFieldException extends RuntimeException {
    public DmlNoSuchFieldException(String field) {
        super("Error: No such field " + field);
    }
}

package com.ericsson.ema.tim.json;

/**
 * Created by eqinson on 2017/3/7.
 */
public class TypeInfo {
    private final String name;
    private final String type;

    TypeInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String toString() {
        return "TypeInfo{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}

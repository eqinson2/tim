package com.ericsson.ema.tim.pojo;

import com.ericsson.ema.tim.pojo.model.Table;
import javassist.CannotCompileException;
import javassist.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eqinson on 2017/4/21.
 */
public class PojoGenerator {
    public final static String pojoPkg = PojoGenerator.class.getPackage().getName();
    private final static Logger LOGGER = LoggerFactory.getLogger(PojoGenerator.class);
    private static Map<String, Class<?>> typesForTuple = new HashMap<>();

    static {
        typesForTuple.put("int", Integer.class);
        typesForTuple.put("string", String.class);
        typesForTuple.put("bool", Boolean.class);
    }

    private PojoGenerator() {
    }

    private static void generateTupleClz(Table table) {
        Map<String, Class<?>> props = table.getRecords().getTuples().stream().collect(
            HashMap<String, Class<?>>::new,
            (map, nameType) -> map.put(nameType.getName(), typesForTuple.get(nameType.getType())),
            (m, u) -> {
            });

        if (LOGGER.isDebugEnabled())
            props.forEach((k, v) -> LOGGER.debug("name: {}, type: {}", k, v.getName()));

        String classToGen = pojoPkg + "." + table.getName() + "Data";
        try {
            PojoGenUtil.generatePojo(classToGen, props);
        } catch (NotFoundException | CannotCompileException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void generateTableClz(Table table) {
        generateTupleClz(table);
        Map<String, Class<?>> props = new HashMap<>();
        props.put("records", List.class);

        String classTOGen = pojoPkg + "." + table.getName();
        Class<?> clz;
        try {
            clz = PojoGenUtil.generateListPojo(classTOGen, props);
        } catch (NotFoundException | CannotCompileException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        Thread.currentThread().setContextClassLoader(clz.getClassLoader());
    }
}
package com.ericsson.ema.tim.pojo;

import javassist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by eqinson on 2017/4/21.
 */
public class PojoGenUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(PojoGenUtil.class);

    public static Class<?> generatePojo(String className, Map<String, Class<?>> properties) throws
        NotFoundException, CannotCompileException {
        CtClass cc = makeClass(className);
        cc.addInterface(resolveCtClass(Serializable.class));
        properties.forEach((k, v) -> {
            try {
                CtField field = new CtField(resolveCtClass(v), k, cc);
                field.setModifiers(Modifier.PRIVATE);
                cc.addField(field);
                cc.addMethod(generatePlainGetter(cc, k, v));
                cc.addMethod(generateSetter(cc, k, v));
            } catch (NotFoundException | CannotCompileException e) {
                LOGGER.error(e.getMessage());
                throw new RuntimeException(e);
            }
        });
        cc.addMethod(generateToString(cc));

        return (Class<?>) cc.toClass(new Loader());
    }

    static Class<?> generateListPojo(String className, Map<String, Class<?>> properties) throws
        NotFoundException, CannotCompileException {
        CtClass cc = makeClass(className);
        cc.addInterface(resolveCtClass(Serializable.class));
        properties.forEach((k, v) -> {
            try {
                CtField field = new CtField(resolveCtClass(v), k, cc);
                field.setModifiers(Modifier.PRIVATE);
                cc.addField(field);
                cc.addMethod(generateListGetter(cc, k, v));
            } catch (NotFoundException | CannotCompileException e) {
                LOGGER.error(e.getMessage());
                throw new RuntimeException(e);
            }
        });

        //make sure share same classloeader with generatePojo
        return (Class<?>) cc.toClass(Thread.currentThread().getContextClassLoader());
    }

    private static CtMethod generatePlainGetter(CtClass declaringClass, String fieldName, Class<?> fieldClass)
        throws CannotCompileException {
        String getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        String sb = String.format("public %s %s() { return this.%s; }", fieldClass.getName(), getterName, fieldName);
        LOGGER.debug("generatePlainGetter:{}", sb);
        return CtMethod.make(sb, declaringClass);
    }

    private static CtMethod generateListGetter(CtClass declaringClass, String fieldName, Class<?> fieldClass)
        throws CannotCompileException {
        String getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        String sb = String.format("public %s %s() { if (%s == null) { %s = new java.util.ArrayList(); } return this.%s; }",
            fieldClass.getName(), getterName, fieldName, fieldName, fieldName);
        LOGGER.debug("generateListGetter:{}", sb);
        return CtMethod.make(sb, declaringClass);
    }

    private static CtMethod generateSetter(CtClass declaringClass, String fieldName, Class<?> fieldClass)
        throws CannotCompileException {
        String setterName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        String sb = String.format("public void %s( %s %s) { this.%s = %s; }", setterName, fieldClass.getName(), fieldName,
            fieldName, fieldName);
        LOGGER.debug("generateSetter:{}", sb);
        return CtMethod.make(sb, declaringClass);
    }

    private static CtMethod generateToString(CtClass declaringClass)
        throws CannotCompileException {
        String toStringBody = Arrays.stream(declaringClass.getDeclaredFields()).map(f -> "\"{\"+String.valueOf(" + f
            .getName() + ")+\"}\"").collect(Collectors.joining("+", "return ", ";"));
        String sb = String.format("public String toString() { %s }", toStringBody);
        LOGGER.debug("generateToString:{}", sb);
        return CtMethod.make(sb, declaringClass);
    }

    private static CtClass resolveCtClass(Class<?> clazz) throws NotFoundException {
        ClassPool pool = ClassPool.getDefault();
        return pool.get(clazz.getName());
    }

    private static CtClass makeClass(String className) throws NotFoundException, CannotCompileException {
        ClassPool pool = ClassPool.getDefault();
        if (pool.getOrNull(className) == null) {
            return pool.makeClass(className);
        } else {
            CtClass ccOld = pool.get(className);
            ccOld.defrost();
            return pool.makeClass(className);
        }
    }
}

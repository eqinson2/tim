package com.ericsson.ema.tim.reflection;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class ReflectionTest {
    private final static int LOOPNUM = 1000;
    private static List<Tuple> list = new ArrayList<>();

    @Before
    public void init() {
        list.add(new Tuple(1, "eqinson", "male", 36));
        list.add(new Tuple(2, "eqinson2", "male", 36));
        list.add(new Tuple(3, "eqinson3", "male", 36));
        list.add(new Tuple(4, "eqinson4", "male", 36));
    }

    public int getTupleId(Tuple tuple, Method getid) {
        int result = -1;
        try {
            result = (Integer) getid.invoke(tuple);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Test
    public void testGetDeclaredMethod() throws NoSuchFieldException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < LOOPNUM; i++)
            Tuple.class.getDeclaredField("name");
        System.out.println(System.currentTimeMillis() - start);
    }


    @Test
    public void testNonReflection() {
        long start = System.currentTimeMillis();
        init();
        for (int i = 0; i < LOOPNUM; i++)
            list.stream().filter(t -> t.getId() == 1).collect(Collectors.toList());
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testReflection() throws NoSuchMethodException {
        long start = System.currentTimeMillis();
        init();
        Method getid = Tuple.class.getDeclaredMethod("getId");
        getid.setAccessible(true);

        for (int i = 0; i < LOOPNUM; i++) {
            list.stream().filter(t -> getTupleId(t, getid) == 1).collect(Collectors.toList());
        }
        System.out.println(System.currentTimeMillis() - start);
    }

}

class Tuple {
    private int id;
    private String name;
    private String gender;
    private int age;

    Tuple(int id, String name, String gender, int age) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Tuple{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", gender='" + gender + '\'' +
            ", age=" + age +
            '}';
    }

    int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

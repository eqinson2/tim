package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.AppMainTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static com.ericsson.ema.tim.dml.Select.select;
import static com.ericsson.ema.tim.dml.predicate.Eq.eq;
import static com.ericsson.ema.tim.dml.predicate.UnEq.uneq;

/**
 * Created by eqinson on 2017/4/24.
 */
public class UnEqTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testUnEq() {
        LOGGER.info("=====================select some data for testing uneq=====================");
        List<Object> result = select().from(tableName).where(uneq("name", "eqinson1")).where(eq("age",
            "1")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson2")).where(uneq("age", "6"))
            .collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(uneq("maintenance", "TRUE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson2"))
                .where(uneq("maintenance", "TRUE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(uneq("maintenance", "FALSE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson2"))
                .where(uneq("maintenance", "FALSE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(uneq("name", "eqinson1"))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job")
            .collectBySelectFields();
        Util.printResult(sliceRes);
        System.out.println();
    }
}

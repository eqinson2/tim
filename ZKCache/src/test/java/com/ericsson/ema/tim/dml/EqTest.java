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

/**
 * Created by eqinson on 2017/4/24.
 */
public class EqTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testEq() {
        LOGGER.info("=====================select some data for testing eq=====================");
        List<Object> result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age",
            "1")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson2")).where(eq("age", "6"))
            .collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "4")).where(eq
            ("job", "manager")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("maintenance", "TRUE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson1"))
                .where(eq("maintenance", "TRUE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("maintenance", "FALSE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson1"))
                .where(eq("maintenance", "FALSE")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "4")).where(eq
            ("job", "manager")).orderBy("name", "asc").orderBy("job", "desc").collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).orderBy("maintenance", "desc").orderBy("name", "asc").collect();
        result.forEach(System.out::println);
        System.out.println();
    }
}

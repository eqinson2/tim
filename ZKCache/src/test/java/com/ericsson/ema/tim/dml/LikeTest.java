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
import static com.ericsson.ema.tim.dml.predicate.Like.like;
import static com.ericsson.ema.tim.dml.predicate.UnEq.uneq;
import static com.ericsson.ema.tim.dml.predicate.UnLike.unlike;

/**
 * Created by eqinson on 2017/4/24.
 */
public class LikeTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testLike() {
        LOGGER.info("=====================select some data for testing like/unlike=====================");
        List<Object> result = select().from(tableName).where(like("name", "eqinson[0-9]")).where(eq("age",
            "1")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(unlike("name", "eqinson[0-9]")).where(uneq("age", "6"))
            .collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(like("job", "^HR+ admin+$")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(unlike("job", "^HR|.*admin$")).collect();
        result.forEach(System.out::println);
        System.out.println();

        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson[0-9]")).where(unlike("job", ".*engineer$"))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job")
            .collectBySelectFields();
        Util.printResult(sliceRes);
    }
}

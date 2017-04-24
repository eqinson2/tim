package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.AppMainTest;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.ericsson.ema.tim.dml.Select.select;
import static com.ericsson.ema.tim.dml.predicate.BiggerThan.gt;
import static com.ericsson.ema.tim.dml.predicate.Eq.eq;
import static com.ericsson.ema.tim.dml.predicate.LessThan.lt;
import static com.ericsson.ema.tim.dml.predicate.Like.like;
import static com.ericsson.ema.tim.dml.predicate.Range.range;
import static com.ericsson.ema.tim.dml.predicate.UnLike.unlike;

/**
 * Created by eqinson on 2017/4/24.
 */
public class ExceptionTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test(expected = DmlNoSuchFieldException.class)
    public void testGroupByDmlNoSuchFieldException() throws Exception {
        LOGGER.info("=====================select some data for testing groupby=====================");
        select().from(tableName).
            where(eq("name1", "eqinson")).where(like("job", "engineer"))
            .where(range("age", 1, 10)).groupBy("name").collectByGroup();

        select().from(tableName).
            where(like("name", "eqinson")).where(like("job1", "engineer"))
            .where(range("age", 1, 10)).groupBy("name").collectByGroup();

        select().from(tableName).
            where(like("name", "eqinson")).where(like("job", "engineer"))
            .where(range("age1", 1, 10)).groupBy("name").collectByGroup();

        select().from(tableName).
            where(like("name", "eqinson")).where(unlike("job1", "engineer"))
            .where(range("age", 1, 10)).groupBy("name").collectByGroup();

        select().from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(gt("age1", 1)).groupBy("name").collectByGroup();

        select().from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(lt("age1", 10)).groupBy("name").collectByGroup();

        select().from(tableName).
            where(like("name", "eqinson")).where(like("job", "engineer"))
            .where(range("age", 1, 10)).groupBy("name1").collectByGroup();
    }

    @Test(expected = DmlBadSyntaxException.class)
    public void testGroupByDmlBadSyntaxException() {
        LOGGER.info("=====================select some data for testing groupby with " +
            "DmlBadSyntaxException=====================");
        select().from(tableName).
            where(like("name", "eqinson")).where(like("job", "engineer"))
            .where(range("age", 1, 6)).groupBy("name").groupBy("age").collectByGroup();
    }
}

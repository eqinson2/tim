package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.AppMainTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static com.ericsson.ema.tim.dml.Select.select;
import static com.ericsson.ema.tim.dml.predicate.Like.like;
import static com.ericsson.ema.tim.dml.predicate.Range.range;

/**
 * Created by eqinson on 2017/4/24.
 */
public class GroupTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testGroupBy() throws Exception {
        LOGGER.info("=====================select some data for testing groupby=====================");
        Map<Object, List<Object>> res = select().from(tableName).
            where(like("name", "eqinson")).where(like("job", "engineer"))
            .where(range("age", 1, 10)).groupBy("name").collectByGroup();
        Util.printResultGroup(res);
    }
}

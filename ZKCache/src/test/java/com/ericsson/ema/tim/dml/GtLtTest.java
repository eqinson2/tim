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
import static com.ericsson.ema.tim.dml.predicate.BiggerThan.gt;
import static com.ericsson.ema.tim.dml.predicate.LessThan.lt;
import static com.ericsson.ema.tim.dml.predicate.Like.like;
import static com.ericsson.ema.tim.dml.predicate.UnLike.unlike;

/**
 * Created by eqinson on 2017/4/24.
 */
public class GtLtTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testGtLt() {
        LOGGER.info("=====================select some data for testing gt/lt=====================");
        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(gt("age", 3)).where(lt("age", 6))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job").limit(2)
            .collectBySelectFields();
        Util.printResult(sliceRes);
        System.out.println();
    }

}

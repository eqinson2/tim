package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.ericsson.ema.tim.dml.Insert.insert;

public class InsertTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(InsertTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testInsert() throws KeeperException.ConnectionLossException {
        LOGGER.info("=====================insert some data=====================");
        insert().into(tableName).add("name", "eqinson111").add("age", "1")
                .add("job", "software engineer").add("hometown", "SH").add("maintenance", "TRUE").executeDebug();
    }

    @Test(expected = DmlBadSyntaxException.class)
    public void testInsertDmlNoSuchFieldException() throws Exception {
        LOGGER.info("=====================insert some data for testing DmlNoSuchFieldException =====================");
        insert().into(tableName).add("name", "eqinson1").add("age", "1")
                .add("job", "software engineer").add("hometown", "SH").add("maintenance", "TRUE").executeDebug();
    }

    @Test(expected = DmlBadSyntaxException.class)
    public void testInsertDmlNoSuchFieldException2() throws Exception {
        LOGGER.info("=====================insert some data for testing DmlNoSuchFieldException =====================");
        insert().into(tableName).add("name", "eqinson1").add("age", "1")
                .add("job", "software engineer").add("hometown", "SH").executeDebug();
    }
}

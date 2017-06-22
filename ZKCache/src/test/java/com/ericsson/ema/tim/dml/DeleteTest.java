package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.ericsson.ema.tim.dml.Delete.delete;
import static com.ericsson.ema.tim.dml.predicate.Eq.eq;

public class DeleteTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(DeleteTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Util.init(testFile, tableName);
    }

    @Test
    public void testDelete() throws KeeperException.ConnectionLossException {
        LOGGER.info("=====================insert some data=====================");
        delete().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "1")).where(eq
                ("job", "software engineer")).where(eq("hometown", "SH")).where(eq("maintenance", "TRUE")).executeDebug();
    }

    @Test(expected = DmlBadSyntaxException.class)
    public void testInsertDmlNoSuchFieldException() throws Exception {
        LOGGER.info("=====================insert some data for testing DmlNoSuchFieldException =====================");
        delete().from(tableName).where(eq("name", "eqinson1111")).where(eq("age", "1")).where(eq
                ("job", "software engineer")).where(eq("hometown", "SH")).where(eq("maintenance", "TRUE")).executeDebug();
    }

    @Test(expected = DmlBadSyntaxException.class)
    public void testInsertDmlNoSuchFieldException2() throws Exception {
        LOGGER.info("=====================insert some data for testing DmlNoSuchFieldException =====================");
        delete().from(tableName).where(eq("name", "eqinson1111")).where(eq("age", "1")).where(eq
                ("job", "software engineer")).where(eq("hometown", "SH")).executeDebug();
    }
}

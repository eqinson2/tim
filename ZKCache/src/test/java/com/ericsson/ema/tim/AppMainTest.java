package com.ericsson.ema.tim;

import com.ericsson.ema.tim.utils.FileUtils;
import com.ericsson.ema.tim.zookeeper.ZKMonitor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ericsson.ema.tim.dml.Select.select;
import static com.ericsson.ema.tim.dml.predicate.BiggerThan.gt;
import static com.ericsson.ema.tim.dml.predicate.Eq.eq;
import static com.ericsson.ema.tim.dml.predicate.LessThan.lt;
import static com.ericsson.ema.tim.dml.predicate.Like.like;
import static com.ericsson.ema.tim.dml.predicate.Range.range;
import static com.ericsson.ema.tim.dml.predicate.UnEq.uneq;
import static com.ericsson.ema.tim.dml.predicate.UnLike.unlike;

public class AppMainTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppMainTest.class);

    public static void main(String[] args) throws Exception {
//        zkConnectionManager.init();
//        ZKMonitor zkMonitor = new ZKMonitor(zkConnectionManager);
//        zkMonitor.start();
//
//        while (!Thread.currentThread().isInterrupted()) {
//            try {
//                Thread.sleep(1000 * 60);
//            } catch (InterruptedException e) {
//                return;
//            }
//        }
    }

    private static void performanceTest(String tableName) {
        long start = System.currentTimeMillis();
        int LOOPNUM = 100000;
        for (int i = 0; i < LOOPNUM; i++) {
            select().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "31")).execute();
            select().from(tableName).where(eq("name", "eqinson2")).where(eq("age", "33")).execute();
            select().from(tableName).where(eq("name", "eqinson4")).where(eq("age", "34")).where(eq
                ("job", "manager")).execute();
            select("age", "job").from(tableName).where(eq("name", "eqinson4"))
                .where(eq("age", "34")).where(eq("job", "manager")).executeWithSelectFields();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void doTest() throws Exception {
        String jfname = "json/test.json";
        LOGGER.info("=====================load json: {}=====================", jfname);
        String tableName = "Eqinson";
        ZKMonitor zm = new ZKMonitor(null);
        zm.doLoad(tableName, FileUtils.readFile(jfname));

        LOGGER.info("=====================select some data for testing eq=====================");
        List<Object> result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age",
            "1")).execute();
        System.out.println(result.size());

        result = select().from(tableName).where(eq("name", "eqinson2")).where(eq("age", "6"))
            .execute();
        System.out.println(result.size());

        result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "4")).where(eq
            ("job", "manager")).execute();
        System.out.println(result.size());

        result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "4")).where(eq
            ("job", "manager")).orderby("name", "asc").orderby("job", "desc").execute();
        result.forEach(System.out::println);

        LOGGER.info("=====================select some data for testing uneq=====================");
        result = select().from(tableName).where(uneq("name", "eqinson1")).where(eq("age",
            "1")).execute();
        System.out.println(result.size());

        result = select().from(tableName).where(eq("name", "eqinson2")).where(uneq("age", "6"))
            .execute();
        System.out.println(result.size());

        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(uneq("name", "eqinson1"))
            .orderby("name", "asc").orderby("age", "desc").orderby("job")
            .executeWithSelectFields();
        printResult(sliceRes);

        LOGGER.info("=====================select some data for testing like/unlike=====================");
        result = select().from(tableName).where(like("name", "eqinson")).where(eq("age",
            "1")).execute();
        System.out.println(result.size());

        result = select().from(tableName).where(unlike("name", "eqinson")).where(uneq("age", "6"))
            .execute();
        System.out.println(result.size());

        sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .orderby("name", "asc").orderby("age", "desc").orderby("job")
            .executeWithSelectFields();
        printResult(sliceRes);

        LOGGER.info("=====================select some data for testing gt/lt=====================");
        sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(gt("age", 3)).where(lt("age", 6))
            .orderby("name", "asc").orderby("age", "desc").orderby("job").limit(2)
            .executeWithSelectFields();
        printResult(sliceRes);

        LOGGER.info("=====================select some data for testing range=====================");
        sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(range("age", 4, 6))
            .orderby("name", "asc").orderby("age", "desc").orderby("job").limit(2)
            .executeWithSelectFields();
        printResult(sliceRes);

        LOGGER.info("=====================performance testing=====================");
//        performanceTest(tableName);
    }

    private void printResult(List<List<Object>> sliceRes) {
        for (Object eachRow : sliceRes) {
            if (eachRow instanceof List<?>) {
                List<Object> row = (List<Object>) eachRow;
                row.forEach(r -> System.out.print(r + "   "));
            }
            System.out.println();
        }
    }
}
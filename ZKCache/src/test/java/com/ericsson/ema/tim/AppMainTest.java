package com.ericsson.ema.tim;

import com.ericsson.ema.tim.utils.FileUtils;
import com.ericsson.ema.tim.zookeeper.ZKMonitor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

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

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

//    public static void main(String[] args) throws Exception {
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
//    }

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(testFile);
        if (url != null) {
            File file = new File(url.getPath());
            LOGGER.info("=====================load json: {}=====================", testFile);
            ZKMonitor zm = new ZKMonitor(null);
            zm.doLoad(tableName, FileUtils.readFile(Paths.get(url.toURI())));
        } else throw new FileNotFoundException(testFile + " not found");
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

        result = select().from(tableName).where(eq("name", "eqinson1")).where(eq("age", "4")).where(eq
            ("job", "manager")).orderBy("name", "asc").orderBy("job", "desc").collect();
        result.forEach(System.out::println);
        System.out.println();
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

        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(uneq("name", "eqinson1"))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job")
            .collectBySelectFields();
        printResult(sliceRes);
        System.out.println();
    }

    @Test
    public void testLike() {
        LOGGER.info("=====================select some data for testing like/unlike=====================");
        List<Object> result = select().from(tableName).where(like("name", "eqinson")).where(eq("age",
            "1")).collect();
        result.forEach(System.out::println);
        System.out.println();

        result = select().from(tableName).where(unlike("name", "eqinson")).where(uneq("age", "6"))
            .collect();
        result.forEach(System.out::println);
        System.out.println();

        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job")
            .collectBySelectFields();
        printResult(sliceRes);
    }

    @Test
    public void testGtLt() {
        LOGGER.info("=====================select some data for testing gt/lt=====================");
        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(gt("age", 3)).where(lt("age", 6))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job").limit(2)
            .collectBySelectFields();
        printResult(sliceRes);
        System.out.println();
    }

    @Test
    public void testRange() {
        LOGGER.info("=====================select some data for testing range=====================");
        List<List<Object>> sliceRes = select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(range("age", 4, 6))
            .orderBy("name", "asc").orderBy("age", "desc").orderBy("job").limit(2)
            .collectBySelectFields();
        printResult(sliceRes);
        System.out.println();
    }

    @Test
    public void testCount() {
        LOGGER.info("=====================select some data for testing count/exists=====================");
        System.out.println(select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(range("age", 4, 6))
            .count());

        System.out.println(select("name", "age", "job").from(tableName).
            where(like("name", "eqinson")).where(unlike("job", "engineer"))
            .where(range("age", 4, 6))
            .exists());
    }

    @Test
    public void doGroupBy() throws Exception {
        LOGGER.info("=====================select some data for testing groupby=====================");
        Map<Object, List<Object>> res = select().from(tableName).
            where(like("name", "eqinson")).where(like("job", "engineer"))
            .where(range("age", 1, 6)).groupBy("name").collectByGroup();
        printResultGroup(res);
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

    private void printResultGroup(Map<Object, List<Object>> mapRes) {
        mapRes.forEach((k, v) -> {
            if (v != null) {
                List<Object> row = (List<Object>) v;
                row.forEach(r -> System.out.print(r + "   "));
            }
            System.out.println();
        });
    }
}
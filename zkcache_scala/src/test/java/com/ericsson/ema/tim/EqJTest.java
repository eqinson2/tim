package com.ericsson.ema.tim;

import com.ericsson.ema.tim.dml.Select;
import com.ericsson.ema.tim.dml.predicate.Eq;
import com.ericsson.ema.tim.dml.predicate.UnEq;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * Created by eqinson on 2017/5/15.
 */
public class EqJTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(EqJTest.class);

    private final static String tableName = "Eqinson";
    private final static String testFile = "test.json";

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        JTestUtil.init(testFile, tableName);
    }

    @Test
    public void testEq() {
        LOGGER.info("=====================select some data for testing eq=====================");
        List<Object> sResult = Select.apply().from(tableName).where(Eq.apply("name", "eqinson1")).where(Eq.apply("age", "1")).collect();
        scala.collection.Iterator it = sResult.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        java.util.List<Object> jResult = JavaConversions.seqAsJavaList(sResult);
        jResult.forEach(System.out::println);

        java.util.List<String> param = Arrays.asList("name", "age", "job");
        List<List<Object>> sliceRes = Select.apply(JavaConversions.asScalaBuffer(param)).from(tableName).where(UnEq.apply("name", "eqinson1"))
                .orderBy("name", "asc").orderBy("age", "desc").orderBy("job", "asc").collectBySelectFields();
        for (List<Object> eachRow : JavaConversions.seqAsJavaList(sliceRes)) {
            java.util.List<Object> row = JavaConversions.seqAsJavaList(eachRow);
            row.forEach(r -> System.out.print(r + "   "));
            System.out.println();
        }
    }
}

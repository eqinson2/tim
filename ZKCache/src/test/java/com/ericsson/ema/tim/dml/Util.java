package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.utils.FileUtils;
import com.ericsson.ema.tim.zookeeper.ZKMonitor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Created by eqinson on 2017/4/24.
 */
public class Util {
    public static void init(String testFile, String tableName) throws IOException, URISyntaxException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(testFile);
        if (url != null) {
            ZKMonitor zm = new ZKMonitor(null);
            zm.doLoad(tableName, FileUtils.readFile(Paths.get(url.toURI())));
        } else throw new FileNotFoundException(testFile + " not found");
    }

    static void printResult(List<List<Object>> sliceRes) {
        for (Object eachRow : sliceRes) {
            if (eachRow instanceof List<?>) {
                List<Object> row = (List<Object>) eachRow;
                row.forEach(r -> System.out.print(r + "   "));
            }
            System.out.println();
        }
    }

    static void printResultGroup(Map<Object, List<Object>> mapRes) {
        mapRes.forEach((k, v) -> {
            if (v != null) {
                List<Object> row = (List<Object>) v;
                row.forEach(r -> System.out.print(r + "   "));
            }
            System.out.println();
        });
    }
}

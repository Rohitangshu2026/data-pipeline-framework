package org.example.datapipeline;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class PipelineTest {

    private static final String PIPELINE_FILE =
            "src/main/resources/pipeline_config/pipeline-join-aggregate-test.xml";

    private static final String OUTPUT_DIR =
            "src/main/resources/output/";

    @Test
    public void testFullPipeline_endToEnd_correctness() throws Exception {

        // 1. Run pipeline
        Main.main(new String[]{PIPELINE_FILE});

        // 2. Validate SUM output
        Map<String, Double> sumMap = readAggregateFile(
                OUTPUT_DIR + "country_sum.csv"
        );

        // Expected (from small dataset)
        assertEquals(1000.0, sumMap.get("India"), 0.001);
        assertEquals(1800.0, sumMap.get("USA"), 0.001);
        assertEquals(1700.0, sumMap.get("UK"), 0.001);

        // 3. Validate AVG output
        Map<String, Double> avgMap = readAggregateFile(
                OUTPUT_DIR + "country_avg.csv"
        );

        assertEquals(250.0, avgMap.get("India"), 0.001);
        assertEquals(600.0, avgMap.get("USA"), 0.001);
        assertEquals(850.0, avgMap.get("UK"), 0.001);

        // 4. Validate MIN
        Map<String, Double> minMap = readAggregateFile(
                OUTPUT_DIR + "country_min.csv"
        );

        assertEquals(100.0, minMap.get("India"), 0.001);
        assertEquals(500.0, minMap.get("USA"), 0.001);
        assertEquals(800.0, minMap.get("UK"), 0.001);

        // 5. Validate MAX
        Map<String, Double> maxMap = readAggregateFile(
                OUTPUT_DIR + "country_max.csv"
        );

        assertEquals(400.0, maxMap.get("India"), 0.001);
        assertEquals(700.0, maxMap.get("USA"), 0.001);
        assertEquals(900.0, maxMap.get("UK"), 0.001);

        // 6. Validate META AGG (max country sum)
        List<String[]> metaRows = readCsv(
                OUTPUT_DIR + "max_country_sum.csv"
        );

        // header + 1 row expected
        assertTrue(metaRows.size() >= 2);

        double maxValue = Double.parseDouble(metaRows.get(1)[0]);
        assertEquals(1800.0, maxValue, 0.001);
    }

    // ---------------- HELPERS ----------------

    private Map<String, Double> readAggregateFile(String path) throws Exception {
        List<String[]> rows = readCsv(path);
        Map<String, Double> map = new HashMap<>();

        for (int i = 1; i < rows.size(); i++) {
            String key = rows.get(i)[0];
            double val = Double.parseDouble(rows.get(i)[1]);
            map.put(key, val);
        }
        return map;
    }

    private List<String[]> readCsv(String path) throws Exception {
        List<String[]> rows = new ArrayList<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                rows.add(line.split(","));
            }
        }

        return rows;
    }
}
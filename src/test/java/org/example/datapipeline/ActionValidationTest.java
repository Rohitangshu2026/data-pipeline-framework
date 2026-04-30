package org.example.datapipeline;

import org.example.datapipeline.cli.Pipeline;
import org.example.datapipeline.config.action.Method;
import org.example.datapipeline.config.action.Param;
import org.example.datapipeline.executor.action.transform.*;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates every action in the pipeline framework:
 *  - 13 transform strategies (filter, select, map, aggregate x5, derive,
 *    drop_nulls, fill_nulls, sort, limit, normalize, scale, max)
 *  - join (inner / hash-join path)
 *  - plugin actions (assign_roll_number, generate_email_id,
 *    http_request mock, generate_pdf) via pipeline XML
 *  - error-handling strategies (proceed, retry)
 */
public class ActionValidationTest {

    // ───────────────────────── helpers ─────────────────────────

    /** Build a Method with optional k/v param pairs. */
    private static Method method(String name, String... kvs) throws Exception {
        Method m = new Method();
        Field nameF = Method.class.getDeclaredField("name");
        nameF.setAccessible(true);
        nameF.set(m, name);

        List<Param> list = new ArrayList<>();
        for (int i = 0; i < kvs.length; i += 2) {
            Param p = new Param();
            Field pn = Param.class.getDeclaredField("name");
            Field pv = Param.class.getDeclaredField("value");
            pn.setAccessible(true); pv.setAccessible(true);
            pn.set(p, kvs[i]); pv.set(p, kvs[i + 1]);
            list.add(p);
        }
        Field paramsF = Method.class.getDeclaredField("params");
        paramsF.setAccessible(true);
        paramsF.set(m, list);
        return m;
    }

    /** Wrap a varargs array of rows into a DataIterator. */
    private static DataIterator rows(String[]... data) {
        return new DataIterator() {
            int idx = 0;
            public boolean hasNext() { return idx < data.length; }
            public String[] next()   { return data[idx++]; }
        };
    }

    /** Drain an iterator into a list (header + data rows). */
    private static List<String[]> drain(DataIterator it) {
        List<String[]> out = new ArrayList<>();
        while (it.hasNext()) out.add(it.next());
        return out;
    }

    /** Convert a list of rows into a map keyed on column[keyIdx]. */
    private static Map<String, String[]> indexBy(List<String[]> rows, int keyIdx) {
        Map<String, String[]> m = new LinkedHashMap<>();
        for (int i = 1; i < rows.size(); i++) m.put(rows.get(i)[keyIdx], rows.get(i));
        return m;
    }

    /** Standard 4-row employee dataset: name, age, salary, department */
    private static DataIterator employees() {
        return rows(
            new String[]{"name",    "age", "salary", "department"},
            new String[]{"Alice",   "30",  "50000",  "Engineering"},
            new String[]{"Bob",     "25",  "40000",  "Marketing"},
            new String[]{"Charlie", "35",  "60000",  "Engineering"},
            new String[]{"Dave",    "28",  "45000",  "Marketing"}
        );
    }

    /** Employee dataset with one null age and one null salary. */
    private static DataIterator employeesWithNulls() {
        return rows(
            new String[]{"name",    "age", "salary"},
            new String[]{"Alice",   "30",  "50000"},
            new String[]{"Bob",     "",    "40000"},
            new String[]{"Charlie", "35",  ""},
            new String[]{"Dave",    "28",  "45000"}
        );
    }

    // ══════════════════════════════════════════════════════════════
    //  FILTER
    // ══════════════════════════════════════════════════════════════

    @Test
    void testFilter_stringEquality() throws Exception {
        DataIterator out = new FilterStrategy()
            .apply(employees(), method("filter", "column", "department", "operator", "=", "value", "Engineering"));
        List<String[]> rows = drain(out);
        assertEquals(3, rows.size()); // header + Alice + Charlie
        assertEquals("Alice",   rows.get(1)[0]);
        assertEquals("Charlie", rows.get(2)[0]);
    }

    @Test
    void testFilter_numericGreaterThan() throws Exception {
        DataIterator out = new FilterStrategy()
            .apply(employees(), method("filter", "column", "age", "operator", ">", "value", "28"));
        List<String[]> rows = drain(out);
        assertEquals(3, rows.size()); // header + Alice(30) + Charlie(35)
        Set<String> names = new HashSet<>(Arrays.asList(rows.get(1)[0], rows.get(2)[0]));
        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Charlie"));
    }

    @Test
    void testFilter_numericLessOrEqual() throws Exception {
        DataIterator out = new FilterStrategy()
            .apply(employees(), method("filter", "column", "age", "operator", "<=", "value", "28"));
        List<String[]> rows = drain(out);
        assertEquals(3, rows.size()); // header + Bob(25) + Dave(28)
        Set<String> names = new HashSet<>(Arrays.asList(rows.get(1)[0], rows.get(2)[0]));
        assertTrue(names.contains("Bob"));
        assertTrue(names.contains("Dave"));
    }

    @Test
    void testFilter_numericLessThan() throws Exception {
        DataIterator out = new FilterStrategy()
            .apply(employees(), method("filter", "column", "salary", "operator", "<", "value", "50000"));
        List<String[]> rows = drain(out);
        assertEquals(3, rows.size()); // header + Bob(40000) + Dave(45000)
    }

    @Test
    void testFilter_numericGreaterOrEqual() throws Exception {
        DataIterator out = new FilterStrategy()
            .apply(employees(), method("filter", "column", "salary", "operator", ">=", "value", "50000"));
        List<String[]> rows = drain(out);
        assertEquals(3, rows.size()); // header + Alice(50000) + Charlie(60000)
    }

    // ══════════════════════════════════════════════════════════════
    //  SELECT
    // ══════════════════════════════════════════════════════════════

    @Test
    void testSelect_subsetOfColumns() throws Exception {
        DataIterator out = new SelectStrategy()
            .apply(employees(), method("select", "columns", "name,salary"));
        List<String[]> rows = drain(out);
        assertEquals(5, rows.size()); // header + 4 data rows
        assertArrayEquals(new String[]{"name", "salary"}, rows.get(0));
        assertEquals("Alice",   rows.get(1)[0]);
        assertEquals("50000",   rows.get(1)[1]);
        assertEquals("Bob",     rows.get(2)[0]);
        assertEquals("40000",   rows.get(2)[1]);
    }

    @Test
    void testSelect_singleColumn() throws Exception {
        DataIterator out = new SelectStrategy()
            .apply(employees(), method("select", "columns", "department"));
        List<String[]> rows = drain(out);
        assertEquals(5, rows.size());
        assertEquals(1, rows.get(0).length);
        assertEquals("department", rows.get(0)[0]);
    }

    @Test
    void testSelect_unknownColumnThrows() throws Exception {
        DataIterator out = new SelectStrategy()
            .apply(employees(), method("select", "columns", "nonexistent"));
        assertThrows(RuntimeException.class, () -> drain(out));
    }

    // ══════════════════════════════════════════════════════════════
    //  MAP
    // ══════════════════════════════════════════════════════════════

    @Test
    void testMap_add() throws Exception {
        DataIterator out = new MapStrategy()
            .apply(employees(), method("map", "column", "salary", "operation", "add", "value", "5000"));
        List<String[]> rows = drain(out);
        assertEquals("55000.0", rows.get(1)[2]); // Alice: 50000 + 5000
        assertEquals("45000.0", rows.get(2)[2]); // Bob:   40000 + 5000
    }

    @Test
    void testMap_multiply() throws Exception {
        DataIterator out = new MapStrategy()
            .apply(employees(), method("map", "column", "salary", "operation", "multiply", "value", "2"));
        List<String[]> rows = drain(out);
        assertEquals("100000.0", rows.get(1)[2]); // Alice: 50000 * 2
        assertEquals("80000.0",  rows.get(2)[2]); // Bob:   40000 * 2
    }

    @Test
    void testMap_subtract() throws Exception {
        DataIterator out = new MapStrategy()
            .apply(employees(), method("map", "column", "age", "operation", "subtract", "value", "5"));
        List<String[]> rows = drain(out);
        assertEquals("25.0", rows.get(1)[1]); // Alice: 30 - 5
        assertEquals("20.0", rows.get(2)[1]); // Bob:   25 - 5
    }

    @Test
    void testMap_divide() throws Exception {
        DataIterator out = new MapStrategy()
            .apply(employees(), method("map", "column", "salary", "operation", "divide", "value", "1000"));
        List<String[]> rows = drain(out);
        assertEquals("50.0", rows.get(1)[2]); // Alice: 50000 / 1000
    }

    // ══════════════════════════════════════════════════════════════
    //  AGGREGATE
    // ══════════════════════════════════════════════════════════════

    @Test
    void testAggregate_sum() throws Exception {
        DataIterator out = new AggregateStrategy().apply(
            employees(), method("aggregate", "group_by", "department", "column", "salary", "operation", "sum"));
        Map<String, String[]> map = indexBy(drain(out), 0);
        assertEquals(110000.0, Double.parseDouble(map.get("Engineering")[1]), 0.001);
        assertEquals(85000.0,  Double.parseDouble(map.get("Marketing")[1]),   0.001);
    }

    @Test
    void testAggregate_avg() throws Exception {
        DataIterator out = new AggregateStrategy().apply(
            employees(), method("aggregate", "group_by", "department", "column", "salary", "operation", "avg"));
        Map<String, String[]> map = indexBy(drain(out), 0);
        assertEquals(55000.0, Double.parseDouble(map.get("Engineering")[1]), 0.001);
        assertEquals(42500.0, Double.parseDouble(map.get("Marketing")[1]),   0.001);
    }

    @Test
    void testAggregate_min() throws Exception {
        DataIterator out = new AggregateStrategy().apply(
            employees(), method("aggregate", "group_by", "department", "column", "salary", "operation", "min"));
        Map<String, String[]> map = indexBy(drain(out), 0);
        assertEquals(50000.0, Double.parseDouble(map.get("Engineering")[1]), 0.001);
        assertEquals(40000.0, Double.parseDouble(map.get("Marketing")[1]),   0.001);
    }

    @Test
    void testAggregate_max() throws Exception {
        DataIterator out = new AggregateStrategy().apply(
            employees(), method("aggregate", "group_by", "department", "column", "salary", "operation", "max"));
        Map<String, String[]> map = indexBy(drain(out), 0);
        assertEquals(60000.0, Double.parseDouble(map.get("Engineering")[1]), 0.001);
        assertEquals(45000.0, Double.parseDouble(map.get("Marketing")[1]),   0.001);
    }

    @Test
    void testAggregate_count() throws Exception {
        DataIterator out = new AggregateStrategy().apply(
            employees(), method("aggregate", "group_by", "department", "column", "salary", "operation", "count"));
        Map<String, String[]> map = indexBy(drain(out), 0);
        assertEquals(2, Integer.parseInt(map.get("Engineering")[1]));
        assertEquals(2, Integer.parseInt(map.get("Marketing")[1]));
    }

    // ══════════════════════════════════════════════════════════════
    //  DERIVE
    // ══════════════════════════════════════════════════════════════

    @Test
    void testDerive_simpleMultiply() throws Exception {
        DataIterator out = new DeriveStrategy().apply(
            employees(), method("derive", "new_column", "bonus", "formula", "salary * 0.1"));
        List<String[]> rows = drain(out);
        assertEquals(5, rows.size());
        assertEquals("bonus", rows.get(0)[4]);
        assertEquals(5000.0,  Double.parseDouble(rows.get(1)[4]), 0.001); // Alice
        assertEquals(4000.0,  Double.parseDouble(rows.get(2)[4]), 0.001); // Bob
    }

    @Test
    void testDerive_complexFormula() throws Exception {
        DataIterator out = new DeriveStrategy().apply(
            employees(), method("derive", "new_column", "score", "formula", "(salary + age) * 2"));
        List<String[]> rows = drain(out);
        // Alice: (50000 + 30) * 2 = 100060
        assertEquals(100060.0, Double.parseDouble(rows.get(1)[4]), 0.001);
    }

    @Test
    void testDerive_divisionAndSubtraction() throws Exception {
        DataIterator out = new DeriveStrategy().apply(
            employees(), method("derive", "new_column", "adj", "formula", "salary - age * 100"));
        List<String[]> rows = drain(out);
        // Alice: 50000 - 30*100 = 50000 - 3000 = 47000  (operator precedence: * before -)
        assertEquals(47000.0, Double.parseDouble(rows.get(1)[4]), 0.001);
    }

    // ══════════════════════════════════════════════════════════════
    //  DROP NULLS
    // ══════════════════════════════════════════════════════════════

    @Test
    void testDropNulls_singleColumn() throws Exception {
        DataIterator out = new DropNullsStrategy()
            .apply(employeesWithNulls(), method("drop_nulls", "columns", "age"));
        List<String[]> rows = drain(out);
        assertEquals(4, rows.size()); // header + Alice + Charlie + Dave (Bob dropped)
        Set<String> names = new HashSet<>();
        for (int i = 1; i < rows.size(); i++) names.add(rows.get(i)[0]);
        assertFalse(names.contains("Bob"));
        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Charlie"));
        assertTrue(names.contains("Dave"));
    }

    @Test
    void testDropNulls_multiColumn() throws Exception {
        DataIterator out = new DropNullsStrategy()
            .apply(employeesWithNulls(), method("drop_nulls", "columns", "age,salary"));
        List<String[]> rows = drain(out);
        // Bob (null age) and Charlie (null salary) dropped → Alice + Dave
        assertEquals(3, rows.size());
        assertEquals("Alice", rows.get(1)[0]);
        assertEquals("Dave",  rows.get(2)[0]);
    }

    @Test
    void testDropNulls_noNullsInColumn_keepsAllRows() throws Exception {
        DataIterator out = new DropNullsStrategy()
            .apply(employees(), method("drop_nulls", "columns", "name"));
        List<String[]> rows = drain(out);
        assertEquals(5, rows.size()); // all rows kept
    }

    @Test
    void testDropNulls_unknownColumnThrows() throws Exception {
        DataIterator out = new DropNullsStrategy()
            .apply(employees(), method("drop_nulls", "columns", "nonexistent"));
        assertThrows(RuntimeException.class, () -> drain(out));
    }

    // ══════════════════════════════════════════════════════════════
    //  FILL NULLS
    // ══════════════════════════════════════════════════════════════

    @Test
    void testFillNulls_replacesEmptyWithValue() throws Exception {
        DataIterator out = new FillNullsStrategy()
            .apply(employeesWithNulls(), method("fill_nulls", "column", "age", "value", "0"));
        List<String[]> rows = drain(out);
        Map<String, String[]> byName = indexBy(rows, 0);
        assertEquals("0",  byName.get("Bob")[1]);     // was empty → 0
        assertEquals("30", byName.get("Alice")[1]);   // unchanged
    }

    @Test
    void testFillNulls_fillSalary() throws Exception {
        DataIterator out = new FillNullsStrategy()
            .apply(employeesWithNulls(), method("fill_nulls", "column", "salary", "value", "-1"));
        List<String[]> rows = drain(out);
        Map<String, String[]> byName = indexBy(rows, 0);
        assertEquals("-1",    byName.get("Charlie")[2]); // was empty → -1
        assertEquals("50000", byName.get("Alice")[2]);   // unchanged
    }

    // ══════════════════════════════════════════════════════════════
    //  SORT
    // ══════════════════════════════════════════════════════════════

    @Test
    void testSort_numericAsc() throws Exception {
        DataIterator out = new SortStrategy()
            .apply(employees(), method("sort", "column", "salary", "order", "asc"));
        List<String[]> rows = drain(out);
        assertEquals("Bob",     rows.get(1)[0]); // 40000
        assertEquals("Dave",    rows.get(2)[0]); // 45000
        assertEquals("Alice",   rows.get(3)[0]); // 50000
        assertEquals("Charlie", rows.get(4)[0]); // 60000
    }

    @Test
    void testSort_numericDesc() throws Exception {
        DataIterator out = new SortStrategy()
            .apply(employees(), method("sort", "column", "salary", "order", "desc"));
        List<String[]> rows = drain(out);
        assertEquals("Charlie", rows.get(1)[0]); // 60000
        assertEquals("Alice",   rows.get(2)[0]); // 50000
        assertEquals("Dave",    rows.get(3)[0]); // 45000
        assertEquals("Bob",     rows.get(4)[0]); // 40000
    }

    @Test
    void testSort_stringAsc() throws Exception {
        DataIterator out = new SortStrategy()
            .apply(employees(), method("sort", "column", "name", "order", "asc"));
        List<String[]> rows = drain(out);
        assertEquals("Alice",   rows.get(1)[0]);
        assertEquals("Bob",     rows.get(2)[0]);
        assertEquals("Charlie", rows.get(3)[0]);
        assertEquals("Dave",    rows.get(4)[0]);
    }

    // ══════════════════════════════════════════════════════════════
    //  LIMIT
    // ══════════════════════════════════════════════════════════════

    @Test
    void testLimit_takesFirstNRows() throws Exception {
        DataIterator out = new LimitStrategy()
            .apply(employees(), method("limit", "count", "2"));
        List<String[]> rows = drain(out);
        assertEquals(3, rows.size()); // header + 2 data rows
        assertEquals("Alice", rows.get(1)[0]);
        assertEquals("Bob",   rows.get(2)[0]);
    }

    @Test
    void testLimit_largerThanDataset_returnsAll() throws Exception {
        DataIterator out = new LimitStrategy()
            .apply(employees(), method("limit", "count", "100"));
        List<String[]> rows = drain(out);
        assertEquals(5, rows.size()); // header + 4
    }

    @Test
    void testLimit_zero_returnsOnlyHeader() throws Exception {
        DataIterator out = new LimitStrategy()
            .apply(employees(), method("limit", "count", "0"));
        List<String[]> rows = drain(out);
        assertEquals(1, rows.size()); // header only
    }

    // ══════════════════════════════════════════════════════════════
    //  NORMALIZE
    // ══════════════════════════════════════════════════════════════

    @Test
    void testNormalize_minMaxScaling() throws Exception {
        // salaries: 40000, 45000, 50000, 60000 → range 20000, min 40000
        DataIterator out = new NormalizeStrategy()
            .apply(employees(), method("normalize", "column", "salary"));
        List<String[]> rows = drain(out);
        Map<String, String[]> byName = indexBy(rows, 0);
        assertEquals(0.0,  Double.parseDouble(byName.get("Bob")[2]),     0.001); // 40000
        assertEquals(0.25, Double.parseDouble(byName.get("Dave")[2]),    0.001); // 45000
        assertEquals(0.5,  Double.parseDouble(byName.get("Alice")[2]),   0.001); // 50000
        assertEquals(1.0,  Double.parseDouble(byName.get("Charlie")[2]), 0.001); // 60000
    }

    @Test
    void testNormalize_allSameValue_outputsZero() throws Exception {
        DataIterator input = rows(
            new String[]{"val"},
            new String[]{"100"},
            new String[]{"100"}
        );
        DataIterator out = new NormalizeStrategy()
            .apply(input, method("normalize", "column", "val"));
        List<String[]> rows = drain(out);
        assertEquals("0.0", rows.get(1)[0]);
        assertEquals("0.0", rows.get(2)[0]);
    }

    // ══════════════════════════════════════════════════════════════
    //  SCALE (z-score)
    // ══════════════════════════════════════════════════════════════

    @Test
    void testScale_zScoreNormalization() throws Exception {
        // salaries: 50000, 40000, 60000, 45000
        // mean = 48750, population stddev ≈ 7395.0
        DataIterator out = new ScaleStrategy()
            .apply(employees(), method("scale", "column", "salary"));
        List<String[]> rows = drain(out);
        // all values must be finite doubles
        for (int i = 1; i < rows.size(); i++) {
            double v = Double.parseDouble(rows.get(i)[2]);
            assertTrue(Double.isFinite(v), "Expected finite z-score, got: " + v);
        }
        // Alice(50000) is above mean → positive z-score
        Map<String, String[]> byName = indexBy(rows, 0);
        assertTrue(Double.parseDouble(byName.get("Alice")[2])   > 0);
        // Bob(40000) is below mean → negative z-score
        assertTrue(Double.parseDouble(byName.get("Bob")[2])     < 0);
        // Charlie(60000) is highest → highest z-score
        assertTrue(Double.parseDouble(byName.get("Charlie")[2]) > Double.parseDouble(byName.get("Alice")[2]));
    }

    @Test
    void testScale_allSameValue_outputsZero() throws Exception {
        DataIterator input = rows(
            new String[]{"val"},
            new String[]{"100"},
            new String[]{"100"}
        );
        DataIterator out = new ScaleStrategy()
            .apply(input, method("scale", "column", "val"));
        List<String[]> rows = drain(out);
        assertEquals("0.0", rows.get(1)[0]);
        assertEquals("0.0", rows.get(2)[0]);
    }

    // ══════════════════════════════════════════════════════════════
    //  MAX
    // ══════════════════════════════════════════════════════════════

    @Test
    void testMax_returnsMaxValue() throws Exception {
        DataIterator out = new MaxStrategy()
            .apply(employees(), method("max", "column", "salary"));
        List<String[]> rows = drain(out);
        assertEquals(2, rows.size());                     // header + 1 result
        assertEquals("max_salary", rows.get(0)[0]);
        assertEquals(60000.0, Double.parseDouble(rows.get(1)[0]), 0.001);
    }

    @Test
    void testMax_singleRow() throws Exception {
        DataIterator input = rows(
            new String[]{"score"},
            new String[]{"42"}
        );
        DataIterator out = new MaxStrategy()
            .apply(input, method("max", "column", "score"));
        List<String[]> rows = drain(out);
        assertEquals(42.0, Double.parseDouble(rows.get(1)[0]), 0.001);
    }

    @Test
    void testMax_allNonNumeric_returnsNaN() throws Exception {
        DataIterator input = rows(
            new String[]{"tag"},
            new String[]{"abc"},
            new String[]{"def"}
        );
        DataIterator out = new MaxStrategy()
            .apply(input, method("max", "column", "tag"));
        List<String[]> rows = drain(out);
        assertEquals("NaN", rows.get(1)[0]);
    }

    // ══════════════════════════════════════════════════════════════
    //  JOIN (inner / hash-join) — integration via pipeline XML
    // ══════════════════════════════════════════════════════════════

    @Test
    void testJoin_inner_hashJoin_correctOutput() throws Exception {
        String outPath = "target/test-output/join_test_out.csv";
        String xml = buildJoinXml(
            "src/main/resources/input/users.csv",
            "src/main/resources/input/orders.csv",
            "user_id", "user_id",
            outPath
        );
        Path xmlFile = writeTempXml("join_test", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            assertTrue(out.size() > 1, "Join output must have at least one data row");

            // Verify header contains left and right columns
            String[] header = out.get(0);
            List<String> headerList = Arrays.asList(header);
            assertTrue(headerList.contains("user_id"),  "Header must have user_id");
            assertTrue(headerList.contains("name"),     "Header must have name");
            assertTrue(headerList.contains("country"),  "Header must have country");
            // Right-side non-key columns are prefixed with right_
            assertTrue(headerList.contains("right_amount")   || headerList.contains("right_order_id"),
                "Header must have right-side columns");

            // Every row must have the same column count as the header
            for (int i = 1; i < out.size(); i++) {
                assertEquals(header.length, out.get(i).length,
                    "Row " + i + " column count mismatch");
            }
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    @Test
    void testJoin_inner_noMatchingRows_outputOnlyHeader() throws Exception {
        Path leftFile  = writeTempCsv("join_left",  "id,val\n1,a\n2,b\n");
        Path rightFile = writeTempCsv("join_right", "id,rval\n9,x\n10,y\n");
        String outPath = "target/test-output/join_no_match_out.csv";
        String xml = buildJoinXml(
            leftFile.toString(), rightFile.toString(),
            "id", "id", outPath
        );
        Path xmlFile = writeTempXml("join_no_match", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            // Only the header row, no data rows
            assertEquals(1, out.size(), "No matching rows — output must be header only");
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(leftFile);
            Files.deleteIfExists(rightFile);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    @Test
    void testJoin_inner_manyToOne() throws Exception {
        // Multiple left rows match a single right row
        Path leftFile  = writeTempCsv("join_m1_left",  "id,name\n1,Alice\n1,Alice2\n2,Bob\n");
        Path rightFile = writeTempCsv("join_m1_right", "id,score\n1,100\n2,200\n");
        String outPath = "target/test-output/join_m1_out.csv";
        String xml = buildJoinXml(
            leftFile.toString(), rightFile.toString(),
            "id", "id", outPath
        );
        Path xmlFile = writeTempXml("join_m1", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            // header + 3 data rows (Alice+100, Alice2+100, Bob+200)
            assertEquals(4, out.size());
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(leftFile);
            Files.deleteIfExists(rightFile);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  ERROR HANDLING — proceed
    // ══════════════════════════════════════════════════════════════

    @Test
    void testErrorHandling_proceed_pipelineContinues() throws Exception {
        // A stage with a bad column that would throw, but on_error=proceed
        // → pipeline must complete without throwing
        String badOutPath = "target/test-output/proceed_bad.csv";
        String goodOutPath = "target/test-output/proceed_good.csv";
        Path leftFile = writeTempCsv("proceed_input", "name,age\nAlice,30\nBob,25\n");
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_proceed'>\n" +
            "  <stage id='bad_stage'>\n" +
            "    <on_error handling_strategy='proceed'/>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + leftFile + "'/></input>\n" +
            "      <action type='transform'><method name='filter'>\n" +
            "        <param name='column' value='nonexistent'/>\n" +
            "        <param name='operator' value='='/>\n" +
            "        <param name='value' value='x'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + badOutPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "  <stage id='good_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + leftFile + "'/></input>\n" +
            "      <action type='transform'><method name='filter'>\n" +
            "        <param name='column' value='age'/>\n" +
            "        <param name='operator' value='>'/>\n" +
            "        <param name='value' value='20'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + goodOutPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
        Path xmlFile = writeTempXml("proceed_test", xml);
        try {
            assertDoesNotThrow(() -> Main.main(new String[]{xmlFile.toString()}));
            List<String[]> good = readCsv(goodOutPath);
            assertTrue(good.size() > 1, "Good stage should have produced output");
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(leftFile);
            Files.deleteIfExists(Path.of(badOutPath));
            Files.deleteIfExists(Path.of(goodOutPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  ERROR HANDLING — retry
    // ══════════════════════════════════════════════════════════════

    @Test
    void testErrorHandling_retry_abortsAfterMaxRetries() throws Exception {
        // A stage that always fails with retry → must throw after exhausting retries
        Path leftFile = writeTempCsv("retry_input", "name,age\nAlice,30\n");
        String outPath = "target/test-output/retry_out.csv";
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_retry'>\n" +
            "  <stage id='retry_stage'>\n" +
            "    <on_error handling_strategy='retry' retry_count='2'/>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + leftFile + "'/></input>\n" +
            "      <action type='transform'><method name='filter'>\n" +
            "        <param name='column' value='nonexistent'/>\n" +
            "        <param name='operator' value='='/>\n" +
            "        <param name='value' value='x'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
        Path xmlFile = writeTempXml("retry_test", xml);
        try {
            // Pipeline.run() propagates the exception; Main.main() swallows it
            assertThrows(Exception.class, () -> Pipeline.run(xmlFile.toString()));
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(leftFile);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  PLUGIN: assign_roll_number
    // ══════════════════════════════════════════════════════════════

    @Test
    void testPlugin_assignRollNumber_addsRollColumn() throws Exception {
        Path input = writeTempCsv("roll_input",
            "name,department\nAlice,CSE\nBob,ECE\nCharlie,CSE\n");
        String outPath = "target/test-output/roll_out.csv";
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_roll'>\n" +
            "  <stage id='roll_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + input + "'/></input>\n" +
            "      <action type='assign_roll_number'><method name='execute'>\n" +
            "        <param name='year' value='2024'/>\n" +
            "        <param name='format' value='{year}{dept_code}{counter:03}'/>\n" +
            "        <param name='dept_code_map' value='CSE:CS,ECE:EE'/>\n" +
            "        <param name='strict_dept_mapping' value='true'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
        Path xmlFile = writeTempXml("roll_test", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            // header + 3 data rows
            assertEquals(4, out.size());
            // roll_number column added
            List<String> hdr = Arrays.asList(out.get(0));
            assertTrue(hdr.contains("roll_number"), "Output must contain roll_number column");
            int rollIdx = hdr.indexOf("roll_number");
            // Roll numbers must follow format 2024CS001, 2024EE001, 2024CS002
            Set<String> rolls = new HashSet<>();
            for (int i = 1; i < out.size(); i++) rolls.add(out.get(i)[rollIdx]);
            assertFalse(rolls.contains("FAILED"), "No roll number should be FAILED");
            assertEquals(3, rolls.size(), "All roll numbers must be unique");
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(input);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  PLUGIN: generate_email_id
    // ══════════════════════════════════════════════════════════════

    @Test
    void testPlugin_generateEmailId_addsEmailColumn() throws Exception {
        Path input = writeTempCsv("email_input",
            "name,roll_number\nAlice Smith,2024CS001\nBob Jones,2024CS002\n");
        String outPath = "target/test-output/email_out.csv";
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_email'>\n" +
            "  <stage id='email_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + input + "'/></input>\n" +
            "      <action type='generate_email_id'><method name='execute'>\n" +
            "        <param name='domain' value='test.edu'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
        Path xmlFile = writeTempXml("email_test", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            assertEquals(3, out.size()); // header + 2 rows
            List<String> hdr = Arrays.asList(out.get(0));
            assertTrue(hdr.contains("institute_email"), "Output must have institute_email column");
            int emailIdx = hdr.indexOf("institute_email");

            // Emails must contain domain and be unique
            String e1 = out.get(1)[emailIdx];
            String e2 = out.get(2)[emailIdx];
            assertTrue(e1.contains("@test.edu"), "Email must contain domain: " + e1);
            assertTrue(e2.contains("@test.edu"), "Email must contain domain: " + e2);
            assertNotEquals(e1, e2, "Emails must be unique");
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(input);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  PLUGIN: http_request (mock mode)
    // ══════════════════════════════════════════════════════════════

    @Test
    void testPlugin_httpRequest_mockMode_addsStatusColumn() throws Exception {
        Path input = writeTempCsv("http_input",
            "name,institute_email,roll_number\nAlice,alice@test.edu,2024CS001\n");
        String outPath = "target/test-output/http_out.csv";
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_http'>\n" +
            "  <stage id='http_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + input + "'/></input>\n" +
            "      <action type='http_request'><method name='execute'>\n" +
            "        <param name='url' value='https://httpbin.org/post'/>\n" +
            "        <param name='method' value='POST'/>\n" +
            "        <param name='output_prefix' value='github'/>\n" +
            "        <param name='headers_json' value='{\"Content-Type\":\"application/json\"}'/>\n" +
            "        <param name='body_template' value='{\"email\":\"{institute_email}\"}'/>\n" +
            "        <param name='response_mapping' value='github_id:id,github_username:login'/>\n" +
            "        <param name='strict_mapping' value='false'/>\n" +
            "        <param name='mock_mode' value='true'/>\n" +
            "        <param name='retry_count' value='0'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
        Path xmlFile = writeTempXml("http_test", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            assertEquals(2, out.size()); // header + 1 row
            List<String> hdr = Arrays.asList(out.get(0));
            assertTrue(hdr.contains("github_status"), "Output must have github_status column");
            int statusIdx = hdr.indexOf("github_status");
            assertEquals("SUCCESS", out.get(1)[statusIdx]);
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(input);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  PLUGIN: generate_pdf
    // ══════════════════════════════════════════════════════════════

    @Test
    void testPlugin_generatePdf_addsStatusAndPathColumns() throws Exception {
        Path input = writeTempCsv("pdf_input",
            "name,roll_number\nAlice,2024CS001\nBob,2024CS002\n");
        String outPath     = "target/test-output/pdf_out.csv";
        String pdfDir      = "target/test-output/pdfs";
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_pdf'>\n" +
            "  <stage id='pdf_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + input + "'/></input>\n" +
            "      <action type='generate_pdf'><method name='execute'>\n" +
            "        <param name='output_dir' value='" + pdfDir + "'/>\n" +
            "        <param name='fields' value='Name:name,Roll:roll_number'/>\n" +
            "        <param name='file_name_template' value='{roll_number}_creds.txt'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outPath + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
        Path xmlFile = writeTempXml("pdf_test", xml);
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            assertEquals(3, out.size()); // header + 2 rows
            List<String> hdr = Arrays.asList(out.get(0));
            assertTrue(hdr.contains("pdf_status"), "Output must have pdf_status column");
            assertTrue(hdr.contains("pdf_path"),   "Output must have pdf_path column");
            int statusIdx = hdr.indexOf("pdf_status");
            int pathIdx   = hdr.indexOf("pdf_path");
            for (int i = 1; i < out.size(); i++) {
                assertEquals("SUCCESS", out.get(i)[statusIdx], "Row " + i + " pdf_status must be SUCCESS");
                assertTrue(Files.exists(Path.of(out.get(i)[pathIdx])),
                    "Generated file must exist: " + out.get(i)[pathIdx]);
            }
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(input);
            // cleanup generated txt files
            File dir = new File(pdfDir);
            if (dir.exists()) for (File f : Objects.requireNonNull(dir.listFiles())) f.delete();
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  FILE / XML HELPERS
    // ══════════════════════════════════════════════════════════════

    private static Path writeTempCsv(String name, String content) throws IOException {
        Path dir = Path.of("target/test-output");
        Files.createDirectories(dir);
        Path p = dir.resolve(name + "_" + System.nanoTime() + ".csv");
        Files.writeString(p, content);
        return p;
    }

    private static Path writeTempXml(String name, String content) throws IOException {
        Path dir = Path.of("target/test-output");
        Files.createDirectories(dir);
        Path p = dir.resolve(name + "_" + System.nanoTime() + ".xml");
        Files.writeString(p, content);
        return p;
    }

    private static List<String[]> readCsv(String path) throws IOException {
        List<String[]> rows = new ArrayList<>();
        Path p = Path.of(path);
        if (!Files.exists(p)) return rows;
        try (BufferedReader br = Files.newBufferedReader(p)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isBlank()) rows.add(line.split(",", -1));
            }
        }
        return rows;
    }

    private static String buildJoinXml(
            String leftSrc, String rightSrc,
            String leftKey, String rightKey,
            String outSrc) {
        return buildJoinXml(leftSrc, rightSrc, leftKey, rightKey, outSrc, "hash");
    }

    private static String buildJoinXml(
            String leftSrc, String rightSrc,
            String leftKey, String rightKey,
            String outSrc, String strategy) {
        return "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='test_join'>\n" +
            "  <stage id='join_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + leftSrc + "'/></input>\n" +
            "      <action type='join'><method name='inner'>\n" +
            "        <param name='left_key'     value='" + leftKey   + "'/>\n" +
            "        <param name='right_key'    value='" + rightKey  + "'/>\n" +
            "        <param name='right_src'    value='" + rightSrc  + "'/>\n" +
            "        <param name='join_strategy' value='" + strategy + "'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outSrc + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
    }

    // ══════════════════════════════════════════════════════════════
    //  TRACKING ITERATOR — counts source.next() calls
    // ══════════════════════════════════════════════════════════════

    private static class TrackingIterator implements DataIterator {
        private final DataIterator inner;
        int nextCalls = 0;

        TrackingIterator(DataIterator inner) { this.inner = inner; }

        @Override public boolean hasNext() { return inner.hasNext(); }
        @Override public String[] next()   { nextCalls++; return inner.next(); }
    }

    // ══════════════════════════════════════════════════════════════
    //  TEST 1: MANY-TO-MANY JOIN EXPLOSION
    // ══════════════════════════════════════════════════════════════

    @Test
    void testJoin_manyToMany_cartesianExplosion() throws Exception {
        // Left : id=1 → 2 rows, id=2 → 1 row
        // Right: id=1 → 3 rows, id=2 → 1 row
        // Expected: (2×3) + (1×1) = 7 data rows
        Path left  = writeTempCsv("m2m_left",  "id,name\n1,Alice\n1,AliceB\n2,Bob\n");
        Path right = writeTempCsv("m2m_right", "id,score\n1,100\n1,200\n1,300\n2,400\n");
        String outPath = "target/test-output/m2m_out.csv";
        Path xmlFile = writeTempXml("m2m", buildJoinXml(
            left.toString(), right.toString(), "id", "id", outPath));
        try {
            Main.main(new String[]{xmlFile.toString()});
            List<String[]> out = readCsv(outPath);
            assertEquals(8, out.size(), "header + 7 exploded rows");

            List<String> hdr = Arrays.asList(out.get(0));
            int nameIdx  = hdr.indexOf("name");
            int scoreIdx = hdr.indexOf("right_score");
            assertTrue(nameIdx  >= 0, "Header must contain 'name'");
            assertTrue(scoreIdx >= 0, "Header must contain 'right_score'");

            long aliceCount  = out.stream().skip(1).filter(r -> "Alice" .equals(r[nameIdx])).count();
            long aliceBCount = out.stream().skip(1).filter(r -> "AliceB".equals(r[nameIdx])).count();
            long bobCount    = out.stream().skip(1).filter(r -> "Bob"   .equals(r[nameIdx])).count();

            assertEquals(3, aliceCount,  "Alice  must appear 3 times (3 right matches)");
            assertEquals(3, aliceBCount, "AliceB must appear 3 times (3 right matches)");
            assertEquals(1, bobCount,    "Bob    must appear 1 time  (1 right match)");

            // Verify all 3 scores appear for Alice
            Set<String> aliceScores = new HashSet<>();
            out.stream().skip(1)
               .filter(r -> "Alice".equals(r[nameIdx]))
               .forEach(r -> aliceScores.add(r[scoreIdx]));
            assertEquals(Set.of("100", "200", "300"), aliceScores);
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(left);
            Files.deleteIfExists(right);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  TEST 2: STREAMING SAFETY — no double iteration
    // ══════════════════════════════════════════════════════════════

    @Test
    void testStreamingSafety_hasNextIsIdempotent_sourceConsumedOnce() throws Exception {
        int sourceTotalRows = 5; // 1 header + 4 data rows in employees()

        // ── FilterStrategy ──
        TrackingIterator src1 = new TrackingIterator(employees());
        DataIterator filtered = new FilterStrategy()
            .apply(src1, method("filter", "column", "age", "operator", ">", "value", "28"));

        List<String[]> out1 = paranoidDrain(filtered);

        // Every source row consumed exactly once despite redundant hasNext() calls
        assertEquals(sourceTotalRows, src1.nextCalls,
            "FilterStrategy: source.next() must be called exactly once per row");
        assertEquals(3, out1.size(), "header + Alice(30) + Charlie(35)");

        // ── DropNullsStrategy ──
        TrackingIterator src2 = new TrackingIterator(employeesWithNulls());
        DataIterator dropped = new DropNullsStrategy()
            .apply(src2, method("drop_nulls", "columns", "age"));

        List<String[]> out2 = paranoidDrain(dropped);

        assertEquals(sourceTotalRows, src2.nextCalls,
            "DropNullsStrategy: source.next() must be called exactly once per row");
        assertEquals(4, out2.size(), "header + Alice + Charlie + Dave (Bob's empty age dropped)");

        // ── SelectStrategy ──
        TrackingIterator src3 = new TrackingIterator(employees());
        DataIterator selected = new SelectStrategy()
            .apply(src3, method("select", "columns", "name,salary"));

        List<String[]> out3 = paranoidDrain(selected);

        assertEquals(sourceTotalRows, src3.nextCalls,
            "SelectStrategy: source.next() must be called exactly once per row");
        assertEquals(5, out3.size(), "header + 4 data rows (select keeps all rows)");
    }

    /** Drain an iterator, calling hasNext() 3× before each next() and 2× after exhaustion. */
    private static List<String[]> paranoidDrain(DataIterator it) {
        List<String[]> out = new ArrayList<>();
        while (it.hasNext()) {
            it.hasNext(); // redundant — must not advance source
            it.hasNext(); // redundant — must not advance source
            out.add(it.next());
        }
        it.hasNext(); // after exhaustion — must be safe
        it.hasNext();
        return out;
    }

    // ══════════════════════════════════════════════════════════════
    //  TEST 3: TEMP FILE CLEANUP AFTER SORT-MERGE JOIN
    // ══════════════════════════════════════════════════════════════

    @Test
    void testSortMergeJoin_tempFilesCleanedUpAfterRun() throws Exception {
        File tmpDir = new File("/tmp");
        File[] before = tmpDir.listFiles((d, n) -> n.startsWith("sort_") && n.endsWith(".csv"));
        int countBefore = (before == null ? 0 : before.length);

        Path left  = writeTempCsv("sm_left",  "id,name\n1,Alice\n2,Bob\n3,Charlie\n");
        Path right = writeTempCsv("sm_right", "id,score\n1,100\n2,200\n3,300\n");
        String outPath = "target/test-output/sm_out.csv";
        Path xmlFile = writeTempXml("sm_test",
            buildJoinXml(left.toString(), right.toString(), "id", "id", outPath, "sort_merge"));
        try {
            Main.main(new String[]{xmlFile.toString()});

            // No new sort_*.csv files should remain in /tmp
            File[] after = tmpDir.listFiles((d, n) -> n.startsWith("sort_") && n.endsWith(".csv"));
            int countAfter = (after == null ? 0 : after.length);
            assertEquals(countBefore, countAfter,
                "Sort-merge join must delete all temp spill files from /tmp");

            // Output must also be correct
            List<String[]> out = readCsv(outPath);
            assertEquals(4, out.size(), "header + 3 joined rows");
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(left);
            Files.deleteIfExists(right);
            Files.deleteIfExists(Path.of(outPath));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  TEST 4: LARGE DATASET — 2019-Nov.csv (~67M rows)
    //
    //  Single read-pass aggregate (no intermediate file) so the
    //  67 GB file is touched exactly once, keeping I/O bounded.
    //  Verifies: no OOM, streaming correctness, known event types
    //  and a sanity-check on total row count.
    // ══════════════════════════════════════════════════════════════

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void testLargeDataset_2019Nov_singlePassAggregate() throws Exception {
        // Columns: event_time,event_type,product_id,category_id,
        //          category_code,brand,price,user_id,user_session
        String inputFile = "src/main/resources/input/2019-Nov.csv";
        String aggOut    = "target/test-output/large_agg_event.csv";

        // Aggregate row count by event_type — one read-pass, tiny output (~4 rows)
        String xml = "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='large_test'>\n" +
            "  <datasources>\n" +
            "    <datasource id='nov_data' type='csv'>\n" +
            "      <param name='src' value='" + inputFile + "'/>\n" +
            "    </datasource>\n" +
            "  </datasources>\n" +
            "  <stage id='agg_by_event_type'>\n" +
            "    <task>\n" +
            "      <input ref='nov_data'/>\n" +
            "      <action type='transform'><method name='aggregate'>\n" +
            "        <param name='group_by'  value='event_type'/>\n" +
            "        <param name='operation' value='count'/>\n" +
            "        <param name='column'    value='product_id'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + aggOut + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";

        Path xmlFile = writeTempXml("large_test", xml);
        long startMs = System.currentTimeMillis();
        try {
            Main.main(new String[]{xmlFile.toString()});
            long elapsedMs = System.currentTimeMillis() - startMs;
            System.out.printf("Large dataset (67M rows) completed in %.1fs%n", elapsedMs / 1000.0);

            List<String[]> agg = readCsv(aggOut);
            // Header + at least view, cart, purchase
            assertTrue(agg.size() >= 4, "Expected ≥3 event-type groups + header");
            assertArrayEquals(new String[]{"event_type", "count_product_id"}, agg.get(0));

            // Build event-type → count map
            Map<String, Long> counts = new HashMap<>();
            for (int i = 1; i < agg.size(); i++) {
                counts.put(agg.get(i)[0], Long.parseLong(agg.get(i)[1]));
            }

            // Known event types must be present with substantial row counts
            assertTrue(counts.containsKey("view"),     "event_type 'view' must exist");
            assertTrue(counts.containsKey("purchase"), "event_type 'purchase' must exist");
            assertTrue(counts.get("view")     > 1_000_000L, "views must exceed 1M rows");
            assertTrue(counts.get("purchase") > 100_000L,   "purchases must exceed 100K rows");

            // Total rows across all groups ≈ total data rows in the file
            long totalCounted = counts.values().stream().mapToLong(Long::longValue).sum();
            assertTrue(totalCounted > 60_000_000L && totalCounted < 70_000_000L,
                "Total counted rows must be ~67M, got: " + totalCounted);
        } finally {
            Files.deleteIfExists(xmlFile);
            Files.deleteIfExists(Path.of(aggOut));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  TEST 5: DETERMINISTIC OUTPUT
    // ══════════════════════════════════════════════════════════════

    @Test
    void testDeterministicOutput_identicalRunsProduceIdenticalOutput() throws Exception {
        // Fixed small input with known purchase rows
        Path input = writeTempCsv("det_input",
            "event_type,product_id,price\n" +
            "purchase,1001,100\n" +
            "view,1002,200\n" +
            "purchase,1003,300\n" +
            "cart,1004,50\n");
        String out1 = "target/test-output/det_out1.csv";
        String out2 = "target/test-output/det_out2.csv";
        Path xml1 = writeTempXml("det1", buildFilterXml(input.toString(), out1));
        Path xml2 = writeTempXml("det2", buildFilterXml(input.toString(), out2));
        try {
            Main.main(new String[]{xml1.toString()});
            Main.main(new String[]{xml2.toString()});

            List<String[]> run1 = readCsv(out1);
            List<String[]> run2 = readCsv(out2);

            assertEquals(run1.size(), run2.size(), "Row count must be identical across runs");
            for (int i = 0; i < run1.size(); i++) {
                assertArrayEquals(run1.get(i), run2.get(i),
                    "Row " + i + " must be byte-identical across runs");
            }

            // Sanity-check expected content: header + 2 purchase rows, in input order
            assertEquals(3, run1.size());
            assertArrayEquals(new String[]{"event_type", "product_id", "price"}, run1.get(0));
            assertArrayEquals(new String[]{"purchase", "1001", "100"}, run1.get(1));
            assertArrayEquals(new String[]{"purchase", "1003", "300"}, run1.get(2));
        } finally {
            Files.deleteIfExists(xml1);
            Files.deleteIfExists(xml2);
            Files.deleteIfExists(input);
            Files.deleteIfExists(Path.of(out1));
            Files.deleteIfExists(Path.of(out2));
        }
    }

    private static String buildFilterXml(String inputSrc, String outputSrc) {
        return "<?xml version='1.0' encoding='UTF-8'?>\n" +
            "<job id='det_test'>\n" +
            "  <stage id='filter_stage'>\n" +
            "    <task>\n" +
            "      <input type='csv'><param name='src' value='" + inputSrc + "'/></input>\n" +
            "      <action type='transform'><method name='filter'>\n" +
            "        <param name='column'   value='event_type'/>\n" +
            "        <param name='operator' value='='/>\n" +
            "        <param name='value'    value='purchase'/>\n" +
            "      </method></action>\n" +
            "      <output type='csv'><param name='src' value='" + outputSrc + "'/></output>\n" +
            "    </task>\n" +
            "  </stage>\n" +
            "</job>";
    }
}

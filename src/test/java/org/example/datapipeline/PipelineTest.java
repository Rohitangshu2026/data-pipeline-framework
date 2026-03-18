package org.example.datapipeline;

import org.example.datapipeline.config.Job;
import org.example.datapipeline.parser.JAXBPipelineParser;
import org.example.datapipeline.util.ConfigNormalizer;
import org.example.datapipeline.validator.SemanticValidator;
import org.example.datapipeline.exception.PipelineValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PipelineTest {

    private static final String[] FILES = {
            "pipeline_invalid_duplicate_dependency.xml",
            "pipeline_invalid_unknown_strategy.xml",
            "pipeline_invalid_retry_missing_count.xml",
            "pipeline_invalid_retry_with_abort.xml",
            "pipeline_invalid_empty_stage_id.xml",
            "pipeline_invalid_deep_cycle.xml",
            "pipeline_invalid_missing_output.xml",
            "pipeline_invalid_missing_action_type.xml",
            "pipeline_valid_diamond.xml",
            "pipeline_valid_disconnected.xml",
            "pipeline_valid_fanout.xml"
    };

    private Job load(String file) throws Exception {

        var resource = getClass()
                .getClassLoader()
                .getResource(file);

        if (resource == null) {
            fail("Test resource missing: " + file);
        }

        JAXBPipelineParser parser = new JAXBPipelineParser();

        Job job = parser.parse(new java.io.File(resource.toURI()).getAbsolutePath());

        SemanticValidator.validate(job);
        ConfigNormalizer.normalize(job);

        // trigger DAG + cycle validation
        job.getExecutionLevels();

        return job;
    }

    // =========================
    // VALID TESTS
    // =========================

    @Test
    void testValidSimple() {
        assertDoesNotThrow(() -> load("pipeline_valid_simple.xml"));
    }

    @Test
    void testValidParallel() {
        assertDoesNotThrow(() -> load("pipeline_valid_parallel.xml"));
    }

    @Test
    void testValidComplex() {
        assertDoesNotThrow(() -> load("pipeline_complex.xml"));
    }

    @Test
    void testValidMultiDependency() {
        assertDoesNotThrow(() -> load("pipeline_valid_complex_multi_dep.xml"));
    }

    @Test
    void testStressLarge() {
        assertDoesNotThrow(() -> load("pipeline_valid_stress_large.xml"));
    }

    @Test
    void testDiamondDag() {
        assertDoesNotThrow(() -> load("pipeline_valid_diamond.xml"));
    }

    @Test
    void testDisconnectedGraph() {
        assertDoesNotThrow(() -> load("pipeline_valid_disconnected.xml"));
    }

    @Test
    void testFanout() {
        assertDoesNotThrow(() -> load("pipeline_valid_fanout.xml"));
    }

    // =========================
    // INVALID TESTS
    // =========================

    @Test
    void testSelfCycle() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_self_cycle.xml"));
    }

    @Test
    void testSimpleCycle() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_cycle.xml"));
    }

    @Test
    void testDuplicateStage() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_duplicate_stage.xml"));
    }

    @Test
    void testMissingDependency() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_missing_dependency.xml"));
    }

    // 🔥 XSD failures → Exception
    @Test
    void testMissingAttribute() {
        assertThrows(Exception.class,
                () -> load("pipeline_invalid_missing_attribute.xml"));
    }

    @Test
    void testWrongOrder() {
        assertThrows(Exception.class,
                () -> load("pipeline_invalid_wrong_order.xml"));
    }

    @Test
    void testNoTask() {
        assertThrows(Exception.class,
                () -> load("pipeline_invalid_no_task.xml"));
    }

    @Test
    void testInvalidRetry() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_retry.xml"));
    }

    @Test
    void testUnknownStrategy() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_unknown_strategy.xml"));
    }

    @Test
    void testRetryMissingCount() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_retry_missing_count.xml"));
    }

    @Test
    void testRetryWithAbort() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_retry_with_abort.xml"));
    }

    @Test
    void testEmptyStageId() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_empty_stage_id.xml"));
    }

    @Test
    void testDeepCycle() {
        assertThrows(RuntimeException.class,
                () -> load("pipeline_invalid_deep_cycle.xml"));
    }

    // 🔥 XSD failures → Exception
    @Test
    void testMissingOutput() {
        assertThrows(PipelineValidationException.class,
                () -> load("pipeline_invalid_missing_output.xml"));
    }

    @Test
    void testMissingActionType() {
        assertThrows(Exception.class,
                () -> load("pipeline_invalid_missing_action_type.xml"));
    }

    // =========================
    // EDGE CASES
    // =========================

    @Test
    void testDuplicateDependency() {
        assertDoesNotThrow(() -> load("pipeline_invalid_duplicate_dependency.xml"));
    }

    @Test
    void testBlankDependency() {
        assertDoesNotThrow(() -> load("pipeline_invalid_blank_dependency.xml"));
    }

    // =========================
    // FILE GUARD TEST
    // =========================

    @Test
    void testAllFilesExist() {
        for (String file : FILES) {
            assertNotNull(
                    getClass().getClassLoader().getResource(file),
                    "Missing file: " + file
            );
        }
    }

    // =========================
    // EXECUTION TESTS
    // =========================

    @Test
    void testPipelineExecution() throws Exception {

        var resource = getClass()
                .getClassLoader()
                .getResource("pipeline_valid_simple.xml");

        assertNotNull(resource);

        JAXBPipelineParser parser = new JAXBPipelineParser();
        Job job = parser.parse(new java.io.File(resource.toURI()).getAbsolutePath());

        SemanticValidator.validate(job);
        ConfigNormalizer.normalize(job);

        assertDoesNotThrow(() ->
                org.example.datapipeline.executor.PipelineExecutor.execute(job)
        );
    }

    // =========================
    // CLI TESTS
    // =========================

    @Test
    void testPipelineCLIValid() throws Exception {

        var resource = getClass()
                .getClassLoader()
                .getResource("pipeline_valid_simple.xml");

        assertNotNull(resource);

        assertDoesNotThrow(() ->
                org.example.datapipeline.cli.Pipeline.run(
                        new java.io.File(resource.toURI()).getAbsolutePath()
                )
        );
    }

    @Test
    void testPipelineCLIInvalid() {
        assertThrows(Exception.class, () ->
                org.example.datapipeline.cli.Pipeline.run("invalid_file.xml")
        );
    }

    // =========================
    // MAIN TESTS
    // =========================

    @Test
    void testMainNoArgs() {
        assertDoesNotThrow(() -> Main.main(new String[]{}));
    }

    @Test
    void testMainValid() {
        assertDoesNotThrow(() ->
                Main.main(new String[]{"src/main/resources/pipeline_valid_simple.xml"}));
    }

    @Test
    void testMainInvalid() {
        assertDoesNotThrow(() ->
                Main.main(new String[]{"src/main/resources/pipeline_invalid_missing_dependency.xml"}));
    }

    // =========================
    // DAG CORRECTNESS TEST
    // =========================

    @Test
    void testExecutionLevelsCorrectness() throws Exception {

        Job job = load("pipeline_valid_diamond.xml");

        var levels = job.getExecutionLevels();

        assertEquals(3, levels.size());

        assertEquals("A", levels.get(0).get(0).getId());

        var level1 = levels.get(1).stream().map(s -> s.getId()).toList();
        assertTrue(level1.contains("B"));
        assertTrue(level1.contains("C"));

        assertEquals("D", levels.get(2).get(0).getId());
    }
}
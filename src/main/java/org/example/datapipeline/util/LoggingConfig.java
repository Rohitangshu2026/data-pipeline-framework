package org.example.datapipeline.util;

import java.io.IOException;
import java.io.File;
import java.util.logging.*;

/**
 * Configures the Java Util Logging (JUL) framework for the pipeline runtime.
 *
 * <p>The framework uses JUL rather than an external logging library to keep the runtime
 * dependency footprint minimal. {@link #setup()} must be called once at startup
 * (typically from {@link org.example.datapipeline.Main#main}) before any logging occurs.
 *
 * <h2>Handlers Configured</h2>
 * <ul>
 *   <li><b>Console handler</b> – writes to {@code System.err} with a compact, colour-coded
 *       single-line format: {@code [HH:mm:ss][LEVEL] message}.
 *       INFO messages are green, WARNING is yellow, SEVERE is red. Set to level INFO by
 *       default; the caller may lower it to FINE (debug) via the root logger.</li>
 *   <li><b>File handler</b> – appends JSON log records to {@code logs/pipeline.log}.
 *       The directory is created if it does not exist. Set to level ALL so that debug
 *       output goes to the file even when the console shows only INFO.</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>{@link #setup()} is designed to be called once from the main thread before any
 * pipeline stage threads are launched. Calling it multiple times would remove the existing
 * handlers and add duplicates; it is not idempotent.
 */
public class LoggingConfig {

    /**
     * Removes all default JUL handlers from the root logger and installs the framework's
     * colour-coded console handler and JSON file handler.
     *
     * <p>After this call, log records at INFO level and above are visible on the console
     * in a compact, colour-coded format. All records (including DEBUG/FINE) are written to
     * {@code logs/pipeline.log} in JSON format for offline analysis.
     *
     * @throws RuntimeException if the log file cannot be created or opened
     */
    public static void setup() {
        Logger rootLogger = Logger.getLogger("");

        // Remove default handlers
        for (Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }

        // -------- Console Handler (clean) --------
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.INFO);
        consoleHandler.setFormatter(new Formatter() {

            private static final String RESET = "\u001B[0m";
            private static final String GREEN = "\u001B[32m";
            private static final String YELLOW = "\u001B[33m";
            private static final String RED = "\u001B[31m";

            @Override
            public String format(LogRecord record) {
                String color;

                switch (record.getLevel().getName()) {
                    case "INFO":
                        color = GREEN;
                        break;
                    case "WARNING":
                        color = YELLOW;
                        break;
                    case "SEVERE":
                        color = RED;
                        break;
                    default:
                        color = RESET;
                }

                return String.format(
                        "%s[%tT][%s] %s%s%n",
                        color,
                        new java.util.Date(record.getMillis()),
                        record.getLevel(),
                        record.getMessage(),
                        RESET
                );
            }
        });

        // -------- File Handler (JSON logs) --------
        File logDir = new File("logs");
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        FileHandler fileHandler;
        try {
            fileHandler = new FileHandler("logs/pipeline.log", true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize file logging", e);
        }

        fileHandler.setLevel(Level.ALL);
        fileHandler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return String.format(
                        "{ \"level\": \"%s\", \"logger\": \"%s\", \"message\": \"%s\" }\n",
                        record.getLevel(),
                        record.getLoggerName(),
                        record.getMessage().replace("\"", "'")
                );
            }
        });

        rootLogger.addHandler(consoleHandler);
        rootLogger.addHandler(fileHandler);
        rootLogger.setLevel(Level.INFO);
    }
}
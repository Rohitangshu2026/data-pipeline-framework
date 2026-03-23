package org.example.datapipeline.util;

import java.io.IOException;
import java.io.File;
import java.util.logging.*;

public class LoggingConfig {

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
                        "%s[%s] %s%s%n",
                        color,
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
package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import java.util.Map;
import java.util.logging.Logger;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;

/**
 * {@link DataWriter} implementation that writes rows as comma-separated values to a file.
 *
 * <p>Registered in {@link DataIORegistry} under the type key {@code "csv"}. Used whenever
 * the framework needs to persist the output of a task (all action types except those that
 * declare {@link org.example.datapipeline.executor.action.ActionExecutor#handlesOwnOutput()}).
 *
 * <p>Required parameter:
 * <ul>
 *   <li>{@code src} – destination file path; parent directories are created if they do not
 *       exist</li>
 * </ul>
 *
 * <p>Each row from the iterator is joined with {@code ","} and written as a single line
 * (platform line separator). The file is opened with {@link java.io.FileWriter} in
 * overwrite mode (existing content is replaced). A {@link java.io.BufferedWriter} reduces
 * I/O system calls for large outputs.
 *
 * <p>The total number of written rows (including the header) is logged at INFO level after
 * the write completes.
 */
public class CsvDataWriter implements DataWriter {
    private static final Logger logger = Logger.getLogger(CsvDataWriter.class.getName());

    @Override
    public String getType() { return "csv"; }

    @Override
    public void writeData(DataIterator it, Map<String, String> params) {
        String src = params.get("src");
        if (src == null) throw new RuntimeException("Missing 'src' parameter for CSV output");
        
        try {
            File file = new File(src);
            if (file.getParentFile() != null) file.getParentFile().mkdirs();

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                int count = 0;
                while (it.hasNext()) {
                    String[] row = it.next();
                    if (row == null) throw new RuntimeException("Iterator returned null row");
                    writer.write(String.join(",", row));
                    writer.newLine();
                    count++;
                }
                logger.info("ROWS_WRITTEN count=" + count + " file=" + src);
            }
        } catch (Exception e) {
            logger.severe(String.format("CSV_WRITE_FAILED file=%s error=%s", src, e.getMessage()));
            throw new RuntimeException("Failed to write CSV: " + src, e);
        }
    }
}

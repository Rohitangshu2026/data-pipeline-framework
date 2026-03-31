package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import java.util.Map;
import java.util.logging.Logger;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;

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

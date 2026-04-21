package org.example.datapipeline.onboarding;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.plugin.ActionPlugin;
import org.example.datapipeline.plugin.Executor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeneratePdfPlugin implements ActionPlugin {

    @Override
    public String getType() {
        return "generate_pdf";
    }

    @Override
    public String getName() {
        return "generate_pdf";
    }

    @Override
    public Executor getExecutor() {
        return ctx -> {
            DataIterator input = ctx.getIterator();
            Map<String, String> params = ctx.getMethod().getParamMap();
            String outputDir = params.getOrDefault("output_dir", "target/generated_pdfs");
            String fieldsStr = params.get("fields");
            String fileNameTemplate = params.get("file_name_template");

            if (fieldsStr == null || fieldsStr.isEmpty()) throw new RuntimeException("Missing 'fields' parameter");
            if (fileNameTemplate == null || fileNameTemplate.isEmpty()) throw new RuntimeException("Missing 'file_name_template' parameter");

            // Parse fields
            List<FieldMap> fields = new ArrayList<>();
            for (String part : fieldsStr.split(",")) {
                String[] kv = part.split(":");
                if (kv.length == 2) {
                    fields.add(new FieldMap(kv[0].trim(), kv[1].trim()));
                }
            }

            new File(outputDir).mkdirs();

            return new DataIterator() {
                boolean headerProcessed = false;
                String[] currentHeader;
                int rowIndex = 0;
                int countIn = 0;
                int countOut = 0;

                @Override
                public boolean hasNext() {
                    boolean has = input.hasNext();
                    if (!has && countIn > 0) {
                        System.out.println("GeneratePdfPlugin: ROWS_IN = " + countIn);
                        System.out.println("GeneratePdfPlugin: ROWS_OUT = " + countOut);
                    }
                    return has;
                }

                @Override
                public String[] next() {
                    countIn++;
                    rowIndex++;
                    if (!headerProcessed) {
                        currentHeader = input.next();
                        List<String> headerList = Arrays.asList(currentHeader);
                        
                        // Validate fields against header
                        for (FieldMap fm : fields) {
                            if (!headerList.contains(fm.columnName)) {
                                throw new RuntimeException("Initialization Error: Missing required column '" + fm.columnName + "' in input header");
                            }
                        }
                        
                        String[] newHeader = new String[currentHeader.length + 2];
                        System.arraycopy(currentHeader, 0, newHeader, 0, currentHeader.length);
                        newHeader[currentHeader.length] = "pdf_status";
                        newHeader[currentHeader.length + 1] = "pdf_path";
                        
                        currentHeader = newHeader;
                        headerProcessed = true;
                        countOut++;
                        return newHeader;
                    }

                    String[] row = input.next();
                    String[] newRow = new String[currentHeader.length];
                    for (int i = 0; i < currentHeader.length - 2; i++) {
                        newRow[i] = i < row.length ? row[i] : "";
                    }

                    Map<String, String> rowMap = new HashMap<>();
                    for (int i = 0; i < currentHeader.length - 2; i++) {
                        if (i < row.length && row[i] != null) {
                            rowMap.put(currentHeader[i], row[i]);
                        }
                    }

                    String status = "FAILED";
                    String outPath = "";

                    // Construct filename
                    String baseName = fileNameTemplate;
                    for (Map.Entry<String, String> entry : rowMap.entrySet()) {
                        baseName = baseName.replace("{" + entry.getKey() + "}", entry.getValue());
                    }
                    if (baseName.contains("{") && baseName.contains("}")) {
                        baseName = "UNKNOWN_" + rowIndex + ".txt";
                    }
                    
                    // Sanitize
                    baseName = baseName.replaceAll("[/\\\\:*?]", "_");

                    File pdfFile = new File(outputDir, baseName);

                    try (FileWriter writer = new FileWriter(pdfFile)) {
                        writer.write("--- Credentials Summary ---\n");
                        for (FieldMap fm : fields) {
                            String val = rowMap.get(fm.columnName);
                            val = (val == null || val.trim().isEmpty()) ? "N/A" : val;
                            writer.write(fm.label + ": " + val + "\n");
                        }
                        status = "SUCCESS";
                        outPath = pdfFile.getAbsolutePath();
                    } catch (IOException e) {
                        System.err.println("Warning: Failed to generate PDF for row " + rowIndex + ": " + e.getMessage());
                    }

                    newRow[currentHeader.length - 2] = status;
                    newRow[currentHeader.length - 1] = outPath;

                    countOut++;
                    return newRow;
                }
            };
        };
    }

    private static class FieldMap {
        String label;
        String columnName;
        FieldMap(String l, String c) { this.label = l; this.columnName = c; }
    }
}

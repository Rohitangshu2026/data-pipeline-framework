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

/**
 * Plugin action that generates a text-based credential document for each student row.
 *
 * <p>Registered under the action type {@code "generate_pdf"} via the
 * {@link java.util.ServiceLoader} SPI. Despite the name, the current implementation writes
 * a plain-text file (simulating a PDF) for each row. This is intentional for the demo –
 * a real PDF library (e.g. iText, Apache PDFBox) could replace the {@link java.io.FileWriter}
 * calls without changing the plugin contract.
 *
 * <h2>Parameters</h2>
 * <ul>
 *   <li>{@code output_dir}        – directory where generated files are written
 *       (created if it does not exist); default: {@code "target/generated_pdfs"}</li>
 *   <li>{@code fields}            – comma-separated {@code label:column} pairs defining which
 *       columns to include and how to label them in the output file
 *       (e.g. {@code "Name:name,Roll Number:roll_number,Email:institute_email"})</li>
 *   <li>{@code file_name_template} – filename template with {@code {column_name}} placeholders
 *       resolved from each row (e.g. {@code "{roll_number}_credentials.txt"})</li>
 * </ul>
 *
 * <h2>Input/Output</h2>
 * <p>Accepts any row schema; columns referenced in {@code fields} or {@code file_name_template}
 * must exist in the header. Appends two columns:
 * <ul>
 *   <li>{@code pdf_status} – {@code "SUCCESS"} or {@code "FAILED"}</li>
 *   <li>{@code pdf_path}   – absolute path of the generated file (empty on failure)</li>
 * </ul>
 *
 * <h2>Error Handling</h2>
 * <p>If file generation fails for a specific row (e.g. disk full), the error is printed
 * to {@code System.err}, the row's {@code pdf_status} is set to {@code "FAILED"}, and
 * processing continues with the next row.
 */
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

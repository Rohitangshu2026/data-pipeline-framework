package org.example.datapipeline.onboarding;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.plugin.ActionPlugin;
import org.example.datapipeline.plugin.Executor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class GenerateEmailIdPlugin implements ActionPlugin {

    @Override
    public String getType() {
        return "generate_email_id";
    }

    @Override
    public String getName() {
        return "generate_email_id";
    }

    @Override
    public Executor getExecutor() {
        return ctx -> {
            DataIterator input = ctx.getIterator();
            Set<String> assignedEmails = ConcurrentHashMap.newKeySet();
            String domain = ctx.getMethod().getParamMap().getOrDefault("domain", "college.edu");

            return new DataIterator() {
                boolean headerProcessed = false;
                int nameColIndex = -1;
                int rollColIndex = -1;

                int countIn = 0;
                int countOut = 0;

                @Override
                public boolean hasNext() {
                    boolean has = input.hasNext();
                    if (!has && countIn > 0) {
                        System.out.println("GenerateEmailIdPlugin: ROWS_IN = " + countIn);
                        System.out.println("GenerateEmailIdPlugin: ROWS_OUT = " + countOut);
                    }
                    return has;
                }

                @Override
                public String[] next() {
                    countIn++;
                    if (!headerProcessed) {
                        String[] header = input.next();
                        for (int i = 0; i < header.length; i++) {
                            if (header[i].equalsIgnoreCase("name")) nameColIndex = i;
                            if (header[i].equalsIgnoreCase("roll_number")) rollColIndex = i;
                        }
                        String[] newHeader = new String[header.length + 1];
                        System.arraycopy(header, 0, newHeader, 0, header.length);
                        newHeader[header.length] = "institute_email";
                        headerProcessed = true;
                        countOut++;
                        return newHeader;
                    }

                    String[] row = input.next();
                    String[] newRow = new String[row.length + 1];
                    System.arraycopy(row, 0, newRow, 0, row.length);

                    String name = nameColIndex != -1 && nameColIndex < row.length ? row[nameColIndex].replaceAll("\\s+", ".").toLowerCase() : "student";
                    String roll = rollColIndex != -1 && rollColIndex < row.length ? row[rollColIndex].toLowerCase() : "000";
                    
                    String baseEmail = name + "." + roll + "@" + domain;
                    String finalEmail = baseEmail;
                    int i = 1;
                    while (assignedEmails.contains(finalEmail)) {
                        finalEmail = name + "." + roll + "_" + i + "@" + domain;
                        i++;
                    }
                    assignedEmails.add(finalEmail);
                    newRow[row.length] = finalEmail;
                    countOut++;

                    return newRow;
                }
            };
        };
    }
}

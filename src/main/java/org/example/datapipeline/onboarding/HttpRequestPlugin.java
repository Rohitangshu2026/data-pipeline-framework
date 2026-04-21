package org.example.datapipeline.onboarding;

import org.example.datapipeline.executor.context.ExecutionContext;
import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.plugin.ActionPlugin;
import org.example.datapipeline.plugin.Executor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpRequestPlugin implements ActionPlugin {

    private static final Logger LOGGER = Logger.getLogger(HttpRequestPlugin.class.getName());
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Override
    public String getType() {
        return "http_request";
    }

    @Override
    public String getName() {
        return "http_request";
    }

    @Override
    public Executor getExecutor() {
        return ctx -> {
            Map<String, String> params = ctx.getMethod().getParamMap();
            String url = params.get("url");
            String method = params.get("method");
            String outputPrefix = params.get("output_prefix");
            String responseMappingStr = params.get("response_mapping");
            String bodyTemplateStr = params.get("body_template");
            String headersJsonStr = params.get("headers_json");
            
            boolean strictMapping = "true".equalsIgnoreCase(params.get("strict_mapping"));
            boolean mockMode = "true".equalsIgnoreCase(params.get("mock_mode"));
            int timeoutMs = Integer.parseInt(params.getOrDefault("timeout_ms", "3000"));
            int retryCount = Integer.parseInt(params.getOrDefault("retry_count", "2"));
            int maxQps = Integer.parseInt(params.getOrDefault("max_qps", "0"));

            if (url == null || url.isEmpty()) throw new RuntimeException("Missing url parameter");
            if (method == null || method.isEmpty()) throw new RuntimeException("Missing method parameter");
            if (outputPrefix == null || outputPrefix.isEmpty()) throw new RuntimeException("Missing output_prefix parameter");
            if (responseMappingStr == null || responseMappingStr.isEmpty()) throw new RuntimeException("Missing response_mapping parameter");
            if ("POST".equalsIgnoreCase(method) && (bodyTemplateStr == null || bodyTemplateStr.isEmpty())) {
                throw new RuntimeException("Missing body_template for POST request");
            }

            Set<String> requiredPlaceholders = new HashSet<>();
            requiredPlaceholders.addAll(extractPlaceholders(bodyTemplateStr));
            requiredPlaceholders.addAll(extractPlaceholders(headersJsonStr));

            List<Mapping> mappings = new ArrayList<>();
            for (String mappingPart : responseMappingStr.split(",")) {
                String[] parts = mappingPart.split(":");
                if (parts.length == 2) {
                    mappings.add(new Mapping(parts[0].trim(), parts[1].trim()));
                }
            }

            DataIterator input = ctx.getIterator();

            return new DataIterator() {
                boolean headerProcessed = false;
                String[] originalHeader;
                String[] outputHeader;
                long lastRequestTime = 0;
                int countIn = 0;
                int countOut = 0;

                @Override
                public boolean hasNext() {
                    boolean has = input.hasNext();
                    if (!has && countIn > 0) {
                        System.out.println("HttpRequestPlugin (" + outputPrefix + "): ROWS_IN = " + countIn);
                        System.out.println("HttpRequestPlugin (" + outputPrefix + "): ROWS_OUT = " + countOut);
                    }
                    return has;
                }

                @Override
                public String[] next() {
                    countIn++;
                    if (!headerProcessed) {
                        originalHeader = input.next();
                        List<String> headerList = Arrays.asList(originalHeader);
                        
                        for (String ph : requiredPlaceholders) {
                            if (!headerList.contains(ph)) {
                                throw new RuntimeException("Validation Failed: Required column '" + ph + "' not found in input data.");
                            }
                        }

                        outputHeader = new String[originalHeader.length + mappings.size() + 2];
                        System.arraycopy(originalHeader, 0, outputHeader, 0, originalHeader.length);
                        
                        int idx = originalHeader.length;
                        for (Mapping m : mappings) {
                            outputHeader[idx++] = m.columnName;
                        }
                        outputHeader[idx++] = outputPrefix + "_status";
                        outputHeader[idx] = outputPrefix + "_error";
                        
                        headerProcessed = true;
                        countOut++;
                        return outputHeader;
                    }

                    String[] row = input.next();
                    String[] newRow = new String[outputHeader.length];
                    // Clean array copy using mapped layout lengths
                    for (int i = 0; i < originalHeader.length; i++) {
                        newRow[i] = (i < row.length && row[i] != null) ? row[i] : "";
                    }

                    Map<String, String> rowMap = new HashMap<>();
                    for (int i = 0; i < originalHeader.length; i++) {
                        if (i < row.length) {
                            rowMap.put(originalHeader[i], row[i]);
                        }
                    }

                    // Rate Limiting Pacing
                    if (maxQps > 0) {
                        applyRateLimit(maxQps);
                    }

                    String status = "FAILED";
                    String error = "";
                    Map<String, String> extractedMap = new HashMap<>();

                    try {
                        JSONObject responseJson;
                        if (mockMode) {
                            responseJson = generateMockResponse(row, originalHeader, mappings);
                            // Process mapping via same paths as realistic request
                        } else {
                            responseJson = executeHttpWithRetry(url, method, headersJsonStr, bodyTemplateStr, rowMap, timeoutMs, retryCount, outputPrefix);
                        }

                        boolean mappingFailure = false;
                        Map<String, String> tempMap = new HashMap<>();
                        
                        for (Mapping m : mappings) {
                            String val = extractValueFromPath(responseJson, m.jsonPath);
                            if (val == null) {
                                if (strictMapping) {
                                    mappingFailure = true;
                                    error = "Missing field: " + m.jsonPath;
                                    break;
                                }
                                val = ""; // empty if false strict
                            }
                            tempMap.put(m.columnName, val);
                        }

                        if (!mappingFailure) {
                            extractedMap.putAll(tempMap);
                            status = "SUCCESS";
                            if (mockMode) {
                                LOGGER.info("HTTP_REQUEST prefix=" + outputPrefix + " status=SUCCESS mock=true");
                            }
                        } else {
                            status = "FAILED";
                            LOGGER.warning("HTTP_REQUEST prefix=" + outputPrefix + " status=FAILED error=" + error.replace(" ", "_"));
                        }

                    } catch (InterruptedException e) {
                        error = "Thread interrupted";
                        LOGGER.warning("HTTP_REQUEST prefix=" + outputPrefix + " status=FAILED error=interrupted attempt=0");
                    } catch (Exception e) {
                        error = e.getMessage();
                        LOGGER.warning("HTTP_REQUEST prefix=" + outputPrefix + " status=FAILED error=" + error.replace(" ", "_"));
                    }

                    int outIdx = originalHeader.length;
                    for (Mapping m : mappings) {
                        newRow[outIdx++] = extractedMap.getOrDefault(m.columnName, "");
                    }
                    newRow[outIdx++] = status;
                    newRow[outIdx] = error;

                    countOut++;
                    return newRow;
                }

                private void applyRateLimit(int qps) {
                    long now = System.currentTimeMillis();
                    long delayMillis = 1000L / qps;
                    long timePassed = now - lastRequestTime;
                    if (timePassed < delayMillis) {
                        try {
                            Thread.sleep(delayMillis - timePassed);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    lastRequestTime = System.currentTimeMillis();
                }
            };
        };
    }

    private JSONObject executeHttpWithRetry(String url, String method, String headersJsonStr, String bodyTemplateStr, Map<String, String> rowMap, int timeoutMs, int retryCount, String prefix) throws Exception {
        
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs));

        if (headersJsonStr != null && !headersJsonStr.isEmpty()) {
            JSONObject headersJson = new JSONObject(headersJsonStr);
            for (String key : headersJson.keySet()) {
                String val = headersJson.getString(key);
                reqBuilder.header(key, replaceStringPlaceholders(val, rowMap));
            }
        }

        if ("POST".equalsIgnoreCase(method)) {
            JSONObject bodyJson = new JSONObject(bodyTemplateStr);
            JSONObject hydratedBody = (JSONObject) recursivelyReplacePlaceholders(bodyJson, rowMap);
            reqBuilder.POST(HttpRequest.BodyPublishers.ofString(hydratedBody.toString()));
        } else {
            reqBuilder.GET();
        }

        HttpRequest request = reqBuilder.build();
        
        int attempt = 0;
        while (true) {
            try {
                attempt++;
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                
                if (status >= 200 && status < 300) {
                    LOGGER.info("HTTP_REQUEST prefix=" + prefix + " status=SUCCESS attempt=" + attempt);
                    return new JSONObject(response.body());
                } else if (status >= 500 && status < 600) {
                    if (attempt > retryCount) {
                        throw new RuntimeException("Server error " + status);
                    }
                } else {
                    throw new RuntimeException("Client error " + status);
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException) throw e;
                if (attempt > retryCount || e.getMessage().startsWith("Client error")) {
                    throw e; 
                }
            }

            long delay = 500L * (1L << (attempt - 1));
            Thread.sleep(delay);
        }
    }

    private String extractValueFromPath(JSONObject json, String path) {
        String[] parts = path.split("\\.");
        Object curr = json;
        for (String part : parts) {
            if (curr instanceof JSONObject && ((JSONObject) curr).has(part)) {
                curr = ((JSONObject) curr).get(part);
            } else {
                return null;
            }
        }
        if (curr == JSONObject.NULL) return null;
        return String.valueOf(curr);
    }

    private JSONObject generateMockResponse(String[] row, String[] originalHeader, List<Mapping> mappings) {
        JSONObject mockOutput = new JSONObject();
        String roll = "000";
        String email = "unassigned";
        for (int i = 0; i < originalHeader.length; i++) {
            if (i < row.length) {
                if ("roll_number".equalsIgnoreCase(originalHeader[i])) roll = row[i];
                if ("institute_email".equalsIgnoreCase(originalHeader[i])) email = row[i];
            }
        }
        String prefix = email.contains("@") ? email.split("@")[0] : email;

        int mappedIdx = 1;
        for (Mapping m : mappings) {
            String val = "";
            if (mappedIdx == 1) val = "mock_id_" + roll;
            else if (mappedIdx == 2) val = prefix + "_mock";
            else val = "mock_val_" + mappedIdx;
            
            injectJSONPath(mockOutput, m.jsonPath, val);
            mappedIdx++;
        }
        return mockOutput;
    }

    private void injectJSONPath(JSONObject json, String path, String value) {
        String[] parts = path.split("\\.");
        JSONObject curr = json;
        for (int i = 0; i < parts.length - 1; i++) {
            if (!curr.has(parts[i])) {
                curr.put(parts[i], new JSONObject());
            }
            curr = curr.getJSONObject(parts[i]);
        }
        curr.put(parts[parts.length - 1], value);
    }

    private Object recursivelyReplacePlaceholders(Object obj, Map<String, String> rowMap) {
        if (obj instanceof String) {
            return replaceStringPlaceholders((String) obj, rowMap);
        } else if (obj instanceof JSONObject) {
            JSONObject json = (JSONObject) obj;
            JSONObject result = new JSONObject();
            for (String key : json.keySet()) {
                result.put(key, recursivelyReplacePlaceholders(json.get(key), rowMap));
            }
            return result;
        } else if (obj instanceof JSONArray) {
            JSONArray arr = (JSONArray) obj;
            JSONArray result = new JSONArray();
            for (int i = 0; i < arr.length(); i++) {
                result.put(recursivelyReplacePlaceholders(arr.get(i), rowMap));
            }
            return result;
        }
        return obj;
    }

    private String replaceStringPlaceholders(String text, Map<String, String> rowMap) {
        String result = text;
        Matcher m = Pattern.compile("\\{([a-zA-Z0-9_]+)\\}").matcher(text);
        while (m.find()) {
            String key = m.group(1);
            String val = rowMap.getOrDefault(key, "");
            result = result.replace("{" + key + "}", val);
        }
        return result;
    }

    private Set<String> extractPlaceholders(String text) {
        Set<String> placeholders = new HashSet<>();
        if (text == null) return placeholders;
        Matcher matcher = Pattern.compile("\\{([a-zA-Z0-9_]+)\\}").matcher(text);
        while (matcher.find()) {
            placeholders.add(matcher.group(1));
        }
        return placeholders;
    }

    private static class Mapping {
        String columnName;
        String jsonPath;
        Mapping(String c, String j) { this.columnName = c; this.jsonPath = j; }
    }
}

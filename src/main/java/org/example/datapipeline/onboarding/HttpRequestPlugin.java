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

/**
 * Plugin action that enriches each row by making a per-row HTTP request and mapping fields
 * from the JSON response back into the data stream.
 *
 * <p>Registered under the action type {@code "http_request"} via the
 * {@link java.util.ServiceLoader} SPI. Useful for live credential provisioning, external
 * ID registration, or any scenario where each row needs to call an external REST API.
 *
 * <h2>Parameters</h2>
 * <ul>
 *   <li>{@code url}              – the HTTP endpoint URL</li>
 *   <li>{@code method}           – HTTP method: {@code "GET"} or {@code "POST"}</li>
 *   <li>{@code output_prefix}    – prefix used for the {@code <prefix>_status} and
 *       {@code <prefix>_error} output columns</li>
 *   <li>{@code response_mapping} – comma-separated {@code column:json.path} pairs for
 *       extracting values from the response JSON (e.g. {@code "lms_id:data.id,lms_handle:data.username"})</li>
 *   <li>{@code body_template}    – JSON body template (POST only) with {@code {column}}
 *       placeholders resolved from row values</li>
 *   <li>{@code headers_json}     – JSON object of HTTP headers (also supports placeholders)</li>
 *   <li>{@code strict_mapping}   – if {@code "true"}, a missing JSON path causes row failure</li>
 *   <li>{@code mock_mode}        – if {@code "true"}, skips the actual HTTP call and returns
 *       predictable mock values (useful for demos and tests)</li>
 *   <li>{@code timeout_ms}       – per-request timeout in milliseconds (default: 3000)</li>
 *   <li>{@code retry_count}      – number of retries on 5xx errors (default: 2,
 *       with exponential backoff)</li>
 *   <li>{@code max_qps}          – optional rate limit; if > 0, paces requests to at most
 *       this many per second via {@code Thread.sleep}</li>
 * </ul>
 *
 * <h2>Input/Output</h2>
 * <p>Passes all original columns through unchanged and appends one column per
 * {@code response_mapping} entry plus the {@code <prefix>_status} and
 * {@code <prefix>_error} columns.
 *
 * <h2>Error Handling</h2>
 * <p>If the HTTP call fails after all retries (or if {@code strict_mapping} rejects
 * a response), the row's status is set to {@code "FAILED"} and the error message is
 * stored in {@code <prefix>_error}. Processing continues with the next row.
 *
 * <h2>Retry Strategy</h2>
 * <p>Retries are attempted for 5xx (server-side) errors only. Client errors (4xx) are
 * not retried. Backoff delay doubles on each attempt: 500ms, 1000ms, 2000ms, …
 */
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

    /**
     * Executes the HTTP request with exponential-backoff retry on server-side errors.
     *
     * <p>Builds the {@link java.net.http.HttpRequest} with headers and body from the
     * templates, injecting row column values into any {@code {placeholder}} tokens.
     * On success (2xx status), the response body is parsed as JSON and returned.
     * On 5xx status, the request is retried up to {@code retryCount} times with a
     * doubling delay starting at 500ms. On 4xx or after exhausting retries, a
     * {@link RuntimeException} is thrown.
     *
     * @param url            the endpoint URL
     * @param method         {@code "GET"} or {@code "POST"}
     * @param headersJsonStr JSON string of header key-value pairs (with placeholders)
     * @param bodyTemplateStr JSON body template string (for POST, with placeholders)
     * @param rowMap         current row values keyed by column name (for placeholder substitution)
     * @param timeoutMs      per-request read timeout in milliseconds
     * @param retryCount     maximum number of retry attempts for 5xx errors
     * @param prefix         output column prefix (used for logging)
     * @return parsed JSON response body
     * @throws Exception if all retries fail or an unrecoverable error occurs
     */
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

    /**
     * Navigates a dot-separated JSON path and returns the leaf value as a string.
     *
     * <p>For example, path {@code "data.user.id"} navigates
     * {@code json → "data" → "user" → "id"} and returns {@code String.valueOf(value)}.
     * Returns {@code null} if any intermediate key is missing or the final value is
     * {@link org.json.JSONObject#NULL}.
     *
     * @param json the root JSON object
     * @param path dot-separated navigation path
     * @return the leaf value as a string, or {@code null} if the path does not exist
     */
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

    /**
     * Generates a deterministic mock JSON response for the given row.
     *
     * <p>Used when {@code mock_mode=true}. Produces predictable values based on
     * {@code roll_number} and {@code institute_email} column values if present, so
     * that demo pipelines can run without a real API server.
     *
     * @param row            current data row
     * @param originalHeader column names for the current row
     * @param mappings       the configured response field mappings
     * @return a mock JSON object with all mapped fields populated
     */
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

    /**
     * Injects a value at a dot-separated path into a JSON object, creating intermediate
     * objects as needed.
     *
     * @param json  the root JSON object to inject into (mutated in place)
     * @param path  dot-separated path (e.g. {@code "data.user.id"})
     * @param value the string value to set at the leaf
     */
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

    /**
     * Recursively replaces {@code {placeholder}} tokens in all string values within a
     * JSON structure (object, array, or plain string).
     *
     * @param obj    the JSON value to process (JSONObject, JSONArray, or String)
     * @param rowMap current row values keyed by column name
     * @return a new JSON structure with all placeholders resolved
     */
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

    /**
     * Replaces all {@code {column_name}} placeholders in a string template with the
     * corresponding values from the current row.
     *
     * @param text   the template string potentially containing {@code {placeholder}} tokens
     * @param rowMap current row values keyed by column name
     * @return the resolved string with all recognised placeholders substituted
     */
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

    /**
     * Extracts all placeholder names from a template string.
     *
     * <p>A placeholder is any token matching the pattern {@code {[a-zA-Z0-9_]+}}.
     * Used during header validation to verify that all required columns are present
     * before processing any data rows.
     *
     * @param text the template string to scan (may be {@code null})
     * @return set of placeholder names found; empty if none or if text is {@code null}
     */
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
